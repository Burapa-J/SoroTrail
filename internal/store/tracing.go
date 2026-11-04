package store

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"

	"github.com/sorotrail/sorotrail/internal/requestid"
	"github.com/sorotrail/sorotrail/internal/telemetry"
)

// Originator classes and bounded-cardinality limits for database query cost attribution (#1278).
const (
	OriginKindRoute        = "route"
	OriginKindJob          = "job"
	OriginKindUnattributed = "unattributed"

	UnattributedOrigin   = "unattributed"
	UnmatchedRouteOrigin = "unmatched"
	SystemTenantBucket   = "system"
	UntenantedBucket     = "untenanted"
	OverflowTenantBucket = "t_other"
	RedactedTenantBucket = "t_redacted"

	// DefaultMaxOriginCostSeries caps the number of distinct route/job origin
	// labels tracked in metrics and in the statement-tagger allowlist.
	DefaultMaxOriginCostSeries = 64

	// DefaultMaxTenantCostSeries caps the number of distinct opaque tenant
	// identifiers ("t_<id>") tracked before excess tenants fold into
	// OverflowTenantBucket ("t_other").
	DefaultMaxTenantCostSeries = 64
)

// Prometheus metrics for database query cost attribution (#1278).
// All label dimensions (origin, origin_kind, tenant, operation, outcome) are
// strictly bounded by closed enums or series caps and never carry contract IDs,
// raw paths, or tenant secrets.
var (
	// DBAttributedQueriesTotal counts store operations attributed to their
	// originating route or background job and opaque tenant bucket.
	DBAttributedQueriesTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "sorotrail_db_attributed_queries_total",
			Help: "Total database queries attributed by bounded origin (route pattern or job id), origin_kind, opaque tenant bucket, store operation, and outcome.",
		},
		[]string{"origin", "origin_kind", "tenant", "operation", "outcome"},
	)

	// DBAttributedQueryDurationSeconds records the wall-clock duration of store
	// operations attributed to their originating route or background job and
	// opaque tenant bucket.
	DBAttributedQueryDurationSeconds = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "sorotrail_db_attributed_query_duration_seconds",
			Help:    "Database query duration in seconds attributed by bounded origin, origin_kind, opaque tenant bucket, and store operation.",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"origin", "origin_kind", "tenant", "operation"},
	)

	// DBAttributedRowsTotal counts rows read or written by store operations,
	// attributed to their originating route or background job and opaque tenant.
	DBAttributedRowsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "sorotrail_db_attributed_rows_total",
			Help: "Total rows read or written by database operations, attributed by bounded origin, origin_kind, opaque tenant bucket, and store operation.",
		},
		[]string{"origin", "origin_kind", "tenant", "operation"},
	)

	// DBAttributedSlowQueriesTotal counts database queries that exceeded the
	// slow-query threshold, attributed to their originating route or job and
	// opaque tenant bucket.
	DBAttributedSlowQueriesTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "sorotrail_db_attributed_slow_queries_total",
			Help: "Total slow database queries exceeding the configured threshold, attributed by bounded origin, origin_kind, opaque tenant bucket, and store operation.",
		},
		[]string{"origin", "origin_kind", "tenant", "operation"},
	)
)

func init() {
	prometheus.MustRegister(
		DBAttributedQueriesTotal,
		DBAttributedQueryDurationSeconds,
		DBAttributedRowsTotal,
		DBAttributedSlowQueriesTotal,
	)
}

// QueryOrigin identifies the HTTP route or background job and opaque tenant
// responsible for a database operation (#1278).
type QueryOrigin struct {
	// Kind is OriginKindRoute ("route"), OriginKindJob ("job"), or OriginKindUnattributed ("unattributed").
	Kind string
	// Origin is the bounded route pattern (e.g. "GET /events", "GET /contracts/{id}/events")
	// or background job identifier (e.g. "ingester", "pruner", "archiver", "reconciler", "auditor", "webhook").
	Origin string
	// TenantID is the opaque tenant identifier ("system", "untenanted", "t_<id>", or "t_other").
	// It never contains a tenant name, API key, or contract ID.
	TenantID string
	// RouteResolver optionally resolves the matched HTTP route pattern lazily
	// at query execution time (after chi has populated RouteContext).
	RouteResolver func() string
}

type queryOriginCtxKey struct{}

// WithQueryOrigin returns a copy of ctx carrying the originating route or
// background worker identity and opaque tenant bucket for database cost attribution.
func WithQueryOrigin(ctx context.Context, origin QueryOrigin) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, queryOriginCtxKey{}, origin)
}

// WithRouteOrigin attaches an HTTP route originator and numeric tenant ID (0 for
// single-tenant / untenanted requests) to ctx. If routeResolver is non-nil, it
// is evaluated at query time to read the matched chi route pattern.
func WithRouteOrigin(ctx context.Context, method, fallbackPath string, tenantID int64, untenanted bool, routeResolver func() string) context.Context {
	tenantBucket := UntenantedBucket
	if !untenanted && tenantID > 0 {
		tenantBucket = "t_" + strconv.FormatInt(tenantID, 10)
	}
	initialOrigin := ""
	if fallbackPath != "" {
		initialOrigin = NormalizeRouteOrigin(method, fallbackPath)
	}
	return WithQueryOrigin(ctx, QueryOrigin{
		Kind:          OriginKindRoute,
		Origin:        initialOrigin,
		TenantID:      tenantBucket,
		RouteResolver: routeResolver,
	})
}

// WithJobOrigin attaches a background task identity ("ingester", "pruner",
// "archiver", "reconciler", "auditor", "webhook") to ctx with TenantID="system".
func WithJobOrigin(ctx context.Context, job string) context.Context {
	ctx = requestid.WithJob(ctx, job)
	return WithQueryOrigin(ctx, QueryOrigin{
		Kind:     OriginKindJob,
		Origin:   SanitizeOriginLabel(OriginKindJob, job),
		TenantID: SystemTenantBucket,
	})
}

// ResolveQueryOrigin extracts and sanitizes the QueryOrigin from ctx.
// When no explicit QueryOrigin was attached via WithQueryOrigin, it inspects
// requestid.FromContext(ctx) and PoolBudget's SubsystemFromContext(ctx) so
// background tasks (ingester, pruner, archiver, reconciler, auditor, webhook)
// are attributed automatically.
func ResolveQueryOrigin(ctx context.Context) (QueryOrigin, bool) {
	if ctx == nil {
		return QueryOrigin{
			Kind:     OriginKindUnattributed,
			Origin:   UnattributedOrigin,
			TenantID: UntenantedBucket,
		}, false
	}
	if raw, ok := ctx.Value(queryOriginCtxKey{}).(QueryOrigin); ok {
		kind := raw.Kind
		if kind != OriginKindRoute && kind != OriginKindJob {
			kind = OriginKindRoute
		}
		originStr := raw.Origin
		if raw.RouteResolver != nil {
			if resolved := raw.RouteResolver(); resolved != "" {
				originStr = resolved
			}
		}
		sanitizedOrigin := SanitizeOriginLabel(kind, originStr)
		sanitizedTenant := SanitizeOpaqueTenantID(raw.TenantID, kind)
		return QueryOrigin{
			Kind:     kind,
			Origin:   sanitizedOrigin,
			TenantID: sanitizedTenant,
		}, true
	}

	if reqID := requestid.FromContext(ctx); requestid.IsKnownJob(reqID) {
		return QueryOrigin{
			Kind:     OriginKindJob,
			Origin:   reqID,
			TenantID: SystemTenantBucket,
		}, true
	}

	if sub, ok := SubsystemFromContext(ctx); ok && sub == SubsystemIngester {
		return QueryOrigin{
			Kind:     OriginKindJob,
			Origin:   requestid.JobIngester,
			TenantID: SystemTenantBucket,
		}, true
	}

	return QueryOrigin{
		Kind:     OriginKindUnattributed,
		Origin:   UnattributedOrigin,
		TenantID: UntenantedBucket,
	}, false
}

// canonicalRoutePatterns is the closed set of registered HTTP route patterns in
// SoroTrail. Normalizing paths against this set guarantees that path probing or
// raw contract/event IDs can never create unbounded metric or pg_stat_statements series.
var canonicalRoutePatterns = map[string]struct{}{
	"/health":                        {},
	"/metrics":                       {},
	"/livez":                         {},
	"/readyz":                        {},
	"/version":                       {},
	"/openapi.json":                  {},
	"/docs":                          {},
	"/graphql":                       {},
	"/graphql/persisted":             {},
	"/graphql/persisted/{id}":        {},
	"/graphiql":                      {},
	"/events":                        {},
	"/events/count":                  {},
	"/events/aggregate":              {},
	"/events/ws":                     {},
	"/events.csv":                    {},
	"/events/{id}":                   {},
	"/events/{id}/raw":               {},
	"/events/{id}/transaction":       {},
	"/contracts":                     {},
	"/contracts/{id}":                {},
	"/contracts/{id}/events":         {},
	"/contracts/{id}/stats":          {},
	"/contracts/{id}/export":         {},
	"/contracts/{id}/spec":           {},
	"/contracts/{id}/spec/history":   {},
	"/contracts/{id}/spec/revert":    {},
	"/addresses/{address}/events":    {},
	"/addresses/{address}/summary":   {},
	"/stats":                         {},
	"/subscriptions":                 {},
	"/subscriptions/{id}":            {},
	"/subscriptions/{id}/deliveries": {},
	"/watched-contracts":             {},
	"/watched-contracts/{id}":        {},
	"/dead-letters":                  {},
	"/dead-letters/{id}":             {},
	"/apikeys":                       {},
	"/apikeys/{id}":                  {},
	"/tenant":                        {},
	"/tenant/usage":                  {},
	"/tenant/watch":                  {},
	"/tenant/watch/{contract_id}":    {},
	"/admin/tenants":                 {},
	"/admin/tenants/{id}":            {},
	"/admin/tenants/{id}/usage":      {},
	"/admin/tenants/{id}/grants":     {},
	"/admin/tenants/{id}/grants/{contract_id}": {},
	"/admin/tenants/{id}/keys":                 {},
	"/admin/keys/{key_id}":                     {},
}

var canonicalBackgroundJobs = map[string]struct{}{
	requestid.JobIngester:   {},
	requestid.JobPruner:     {},
	requestid.JobArchiver:   {},
	requestid.JobReconciler: {},
	requestid.JobAuditor:    {},
	requestid.JobWebhook:    {},
	"backfill":              {},
	"maintenance":           {},
}

// ContainsSensitiveIdentifier reports whether s contains a Stellar contract ID
// (C... 56-char strkey), Stellar account address (G... 56-char strkey), API key
// (st_... or sorotrail_...), 64-char hex transaction hash, or URL credential.
func ContainsSensitiveIdentifier(s string) bool {
	if s == "" {
		return false
	}
	if strings.Contains(s, "st_") || strings.Contains(s, "sorotrail_") || strings.Contains(s, "://") {
		return true
	}
	fields := strings.FieldsFunc(s, func(r rune) bool {
		return r == '/' || r == ' ' || r == ',' || r == '=' || r == ':' || r == '?' || r == '&' || r == ';'
	})
	for _, tok := range fields {
		if isStellarStrkey(tok) || isHexHash64(tok) {
			return true
		}
	}
	return false
}

func isStellarStrkey(tok string) bool {
	if len(tok) != 56 || (tok[0] != 'C' && tok[0] != 'G') {
		return false
	}
	for i := 1; i < len(tok); i++ {
		c := tok[i]
		if (c < 'A' || c > 'Z') && (c < '2' || c > '7') {
			return false
		}
	}
	return true
}

func isHexHash64(tok string) bool {
	if len(tok) != 64 {
		return false
	}
	for i := 0; i < len(tok); i++ {
		c := tok[i]
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') && (c < 'A' || c > 'F') {
			return false
		}
	}
	return true
}

// NormalizeRouteOrigin converts an HTTP method and raw or pattern path into a
// bounded route label (e.g. "GET /contracts/{id}/events" or "GET unmatched").
// Path parameters, contract IDs, addresses, and query strings are stripped so
// they never enter metric labels or SQL statement tags.
func NormalizeRouteOrigin(method, rawPath string) string {
	m := strings.ToUpper(strings.TrimSpace(method))
	switch m {
	case "GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS":
	default:
		m = ""
	}

	p := strings.TrimSpace(rawPath)
	if idx := strings.IndexAny(p, "?#"); idx >= 0 {
		p = p[:idx]
	}
	if p == "" {
		if m != "" {
			return m + " " + UnmatchedRouteOrigin
		}
		return UnmatchedRouteOrigin
	}
	if _, ok := canonicalRoutePatterns[p]; ok {
		if m != "" {
			return m + " " + p
		}
		return p
	}

	pattern := collapsePathSegmentsToPattern(p)
	if _, ok := canonicalRoutePatterns[pattern]; ok {
		if m != "" {
			return m + " " + pattern
		}
		return pattern
	}
	if m != "" {
		return m + " " + UnmatchedRouteOrigin
	}
	return UnmatchedRouteOrigin
}

func collapsePathSegmentsToPattern(p string) string {
	trimmed := strings.Trim(p, "/")
	if trimmed == "" {
		return UnmatchedRouteOrigin
	}
	segs := strings.Split(trimmed, "/")
	switch segs[0] {
	case "events":
		if len(segs) == 2 && segs[1] != "count" && segs[1] != "aggregate" && segs[1] != "ws" {
			return "/events/{id}"
		}
		if len(segs) == 3 && (segs[2] == "raw" || segs[2] == "transaction") {
			return "/events/{id}/" + segs[2]
		}
	case "contracts":
		if len(segs) == 2 {
			return "/contracts/{id}"
		}
		if len(segs) == 3 {
			switch segs[2] {
			case "events", "stats", "export", "spec":
				return "/contracts/{id}/" + segs[2]
			}
		}
		if len(segs) == 4 && segs[2] == "spec" && (segs[3] == "history" || segs[3] == "revert") {
			return "/contracts/{id}/spec/" + segs[3]
		}
	case "addresses":
		if len(segs) == 3 && (segs[2] == "events" || segs[2] == "summary") {
			return "/addresses/{address}/" + segs[2]
		}
	case "subscriptions":
		if len(segs) == 2 {
			return "/subscriptions/{id}"
		}
		if len(segs) == 3 && segs[2] == "deliveries" {
			return "/subscriptions/{id}/deliveries"
		}
	case "watched-contracts":
		if len(segs) == 2 {
			return "/watched-contracts/{id}"
		}
	case "dead-letters":
		if len(segs) == 2 {
			return "/dead-letters/{id}"
		}
	case "apikeys":
		if len(segs) == 2 {
			return "/apikeys/{id}"
		}
	case "graphql":
		if len(segs) == 3 && segs[1] == "persisted" {
			return "/graphql/persisted/{id}"
		}
	case "tenant":
		if len(segs) == 3 && segs[1] == "watch" {
			return "/tenant/watch/{contract_id}"
		}
	case "admin":
		if len(segs) == 3 && segs[1] == "tenants" {
			return "/admin/tenants/{id}"
		}
		if len(segs) == 4 && segs[1] == "tenants" && (segs[3] == "usage" || segs[3] == "grants" || segs[3] == "keys") {
			return "/admin/tenants/{id}/" + segs[3]
		}
		if len(segs) == 5 && segs[1] == "tenants" && segs[3] == "grants" {
			return "/admin/tenants/{id}/grants/{contract_id}"
		}
		if len(segs) == 3 && segs[1] == "keys" {
			return "/admin/keys/{key_id}"
		}
	}
	return UnmatchedRouteOrigin
}

// SanitizeOriginLabel enforces bounded cardinality and strips any sensitive
// tokens from a route or background job label.
func SanitizeOriginLabel(kind, raw string) string {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		if kind == OriginKindJob {
			return "maintenance"
		}
		return UnattributedOrigin
	}
	if kind == OriginKindJob {
		lower := strings.ToLower(trimmed)
		if _, ok := canonicalBackgroundJobs[lower]; ok {
			return lower
		}
		return "maintenance"
	}

	// Route kind: split optional HTTP method prefix from path/pattern.
	method := ""
	pathPart := trimmed
	if sp := strings.IndexByte(trimmed, ' '); sp > 0 {
		method = trimmed[:sp]
		pathPart = strings.TrimSpace(trimmed[sp+1:])
	}
	return NormalizeRouteOrigin(method, pathPart)
}

// SanitizeOpaqueTenantID validates that raw is an opaque numeric tenant bucket
// ("t_<positive_int>"), "system", "untenanted", or "t_other". Any free-form
// tenant name, contract strkey, or credential is scrubbed to RedactedTenantBucket.
func SanitizeOpaqueTenantID(raw, kind string) string {
	if kind == OriginKindJob {
		return SystemTenantBucket
	}
	trimmed := strings.TrimSpace(raw)
	switch trimmed {
	case "", "0", UntenantedBucket:
		return UntenantedBucket
	case SystemTenantBucket:
		return SystemTenantBucket
	case OverflowTenantBucket:
		return OverflowTenantBucket
	case RedactedTenantBucket:
		return RedactedTenantBucket
	}
	if ContainsSensitiveIdentifier(trimmed) {
		return RedactedTenantBucket
	}
	numPart := strings.TrimPrefix(trimmed, "t_")
	id, err := strconv.ParseInt(numPart, 10, 64)
	if err != nil || id <= 0 {
		return RedactedTenantBucket
	}
	return "t_" + strconv.FormatInt(id, 10)
}

func sanitizeSQLCommentToken(s string) string {
	var b strings.Builder
	b.Grow(len(s))
	for i := 0; i < len(s); i++ {
		c := s[i]
		if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') ||
			c == '_' || c == '-' || c == '/' || c == '{' || c == '}' || c == '.' || c == ' ' {
			b.WriteByte(c)
		}
	}
	return strings.TrimSpace(b.String())
}

// FormatSQLCommentTag builds a bounded SQL comment tag suitable for joining
// application telemetry with Postgres pg_stat_statements and pg_stat_activity:
//
//	/*sorotrail:origin=GET /events,op=store.QueryEvents,tenant=t_42*/
//
// Because origin, op, and tenant are drawn from bounded, sanitized sets (and
// DefaultCostLedger folds excess tenants past DefaultMaxTenantCostSeries into
// "t_other"), the comment never leaks contract IDs or credentials and cannot
// cause unbounded statement eviction in pg_stat_statements.
func FormatSQLCommentTag(ctx context.Context, op string) string {
	origin, _ := ResolveQueryOrigin(ctx)
	boundedOrigin, boundedTenant := DefaultCostLedger().BindSeries(origin.Origin, origin.Kind, origin.TenantID)
	if op == "" {
		if qn, ok := ctx.Value(queryNameContextKey{}).(string); ok && qn != "" {
			op = qn
		} else {
			op = "store.query"
		}
	}
	safeOrigin := sanitizeSQLCommentToken(boundedOrigin)
	safeOp := sanitizeSQLCommentToken(op)
	safeTenant := sanitizeSQLCommentToken(boundedTenant)
	return fmt.Sprintf("/*sorotrail:origin=%s,op=%s,tenant=%s*/", safeOrigin, safeOp, safeTenant)
}

// TagSQLWithOrigin appends FormatSQLCommentTag(ctx, op) to sqlQuery when ctx
// carries an attributed route or background job origin (or when op is non-empty
// and ctx is attributed). Keeping the comment at the end of the SQL statement
// preserves leading SQL keyword readability and ensures comment-stripping and
// statement-tagging parsers see a deterministic key=value suffix.
func TagSQLWithOrigin(ctx context.Context, op, sqlQuery string) string {
	trimmed := strings.TrimSpace(sqlQuery)
	if trimmed == "" {
		return sqlQuery
	}
	if strings.Contains(trimmed, "/*sorotrail:origin=") {
		return sqlQuery
	}
	_, attributed := ResolveQueryOrigin(ctx)
	if !attributed {
		return sqlQuery
	}
	tag := FormatSQLCommentTag(ctx, op)
	return sqlQuery + " " + tag
}

// AttributedQuery wraps a SQL query and its bind arguments as a pgx.QueryRewriter
// that attaches SoroTrail's bounded cost-attribution SQL comment when executed.
type AttributedQuery struct {
	Op   string
	SQL  string
	Args []any
}

var _ pgx.QueryRewriter = AttributedQuery{}

// RewriteQuery implements pgx.QueryRewriter.
func (q AttributedQuery) RewriteQuery(ctx context.Context, _ *pgx.Conn, sql string, args []any) (string, []any, error) {
	baseSQL := q.SQL
	if baseSQL == "" {
		baseSQL = sql
	}
	baseArgs := q.Args
	if baseArgs == nil {
		baseArgs = args
	}
	return TagSQLWithOrigin(ctx, q.Op, baseSQL), baseArgs, nil
}

// OriginCostSummary aggregates database execution cost for one bounded origin
// (an HTTP route pattern or a background job).
type OriginCostSummary struct {
	Origin          string `json:"origin"`
	OriginKind      string `json:"origin_kind"`
	Queries         uint64 `json:"queries"`
	Errors          uint64 `json:"errors"`
	SlowQueries     uint64 `json:"slow_queries"`
	RowsProcessed   uint64 `json:"rows_processed"`
	TotalDurationNs int64  `json:"total_duration_ns"`
	MaxDurationNs   int64  `json:"max_duration_ns"`
}

// TenantCostSummary aggregates database execution cost for one opaque tenant
// bucket ("system", "untenanted", "t_<id>", or "t_other").
type TenantCostSummary struct {
	TenantID        string `json:"tenant_id"`
	Queries         uint64 `json:"queries"`
	Errors          uint64 `json:"errors"`
	SlowQueries     uint64 `json:"slow_queries"`
	RowsProcessed   uint64 `json:"rows_processed"`
	TotalDurationNs int64  `json:"total_duration_ns"`
	MaxDurationNs   int64  `json:"max_duration_ns"`
}

// CostAttributionSnapshot is a point-in-time view of database cost by route/job
// origin and by tenant, suitable for operator dashboards and quota tuning.
type CostAttributionSnapshot struct {
	MaxOriginSeries      int                 `json:"max_origin_series"`
	MaxTenantSeries      int                 `json:"max_tenant_series"`
	OriginOverflowEvents uint64              `json:"origin_overflow_events"`
	TenantOverflowEvents uint64              `json:"tenant_overflow_events"`
	ByOrigin             []OriginCostSummary `json:"by_origin"`
	ByTenant             []TenantCostSummary `json:"by_tenant"`
}

// CostAttributionLedger maintains bounded in-memory cost aggregates per route/job
// origin and per opaque tenant identifier (#1278).
type CostAttributionLedger struct {
	mu                   sync.RWMutex
	maxOrigins           int
	maxTenants           int
	originOverflowEvents uint64
	tenantOverflowEvents uint64
	origins              map[string]*OriginCostSummary
	tenants              map[string]*TenantCostSummary
}

var defaultCostLedger = NewCostAttributionLedger(DefaultMaxOriginCostSeries, DefaultMaxTenantCostSeries)

// DefaultCostLedger returns the process-wide database cost attribution ledger.
func DefaultCostLedger() *CostAttributionLedger {
	return defaultCostLedger
}

// NewCostAttributionLedger creates a bounded cost attribution ledger with the
// given series caps (<= 0 uses DefaultMaxOriginCostSeries / DefaultMaxTenantCostSeries).
func NewCostAttributionLedger(maxOrigins, maxTenants int) *CostAttributionLedger {
	if maxOrigins <= 0 {
		maxOrigins = DefaultMaxOriginCostSeries
	}
	if maxTenants <= 0 {
		maxTenants = DefaultMaxTenantCostSeries
	}
	return &CostAttributionLedger{
		maxOrigins: maxOrigins,
		maxTenants: maxTenants,
		origins:    make(map[string]*OriginCostSummary, maxOrigins),
		tenants:    make(map[string]*TenantCostSummary, maxTenants),
	}
}

// Reset clears accumulated cost counters. Intended for deterministic tests.
func (l *CostAttributionLedger) Reset() {
	if l == nil {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.originOverflowEvents = 0
	l.tenantOverflowEvents = 0
	clear(l.origins)
	clear(l.tenants)
}

// BindSeries maps (origin, kind, tenantID) to their bounded series keys, folding
// new origins beyond maxOrigins into UnmatchedRouteOrigin and new tenants beyond
// maxTenants into OverflowTenantBucket ("t_other").
func (l *CostAttributionLedger) BindSeries(origin, kind, tenantID string) (boundedOrigin, boundedTenant string) {
	boundedOrigin = SanitizeOriginLabel(kind, origin)
	boundedTenant = SanitizeOpaqueTenantID(tenantID, kind)
	if l == nil {
		return boundedOrigin, boundedTenant
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	if _, exists := l.origins[boundedOrigin]; !exists {
		if len(l.origins) >= l.maxOrigins {
			l.originOverflowEvents++
			boundedOrigin = UnmatchedRouteOrigin
		}
		if _, fallbackExists := l.origins[boundedOrigin]; !fallbackExists {
			l.origins[boundedOrigin] = &OriginCostSummary{
				Origin:     boundedOrigin,
				OriginKind: kind,
			}
		}
	}

	if _, exists := l.tenants[boundedTenant]; !exists {
		if len(l.tenants) >= l.maxTenants {
			l.tenantOverflowEvents++
			boundedTenant = OverflowTenantBucket
		}
		if _, fallbackExists := l.tenants[boundedTenant]; !fallbackExists {
			l.tenants[boundedTenant] = &TenantCostSummary{
				TenantID: boundedTenant,
			}
		}
	}

	return boundedOrigin, boundedTenant
}

// RecordQuery records a completed database operation's duration, row count, and
// error status in both the bounded in-memory ledger and Prometheus metrics.
func (l *CostAttributionLedger) RecordQuery(ctx context.Context, operation string, duration time.Duration, rows int64, err error) (QueryOrigin, bool) {
	origin, attributed := ResolveQueryOrigin(ctx)
	boundedOrigin, boundedTenant := l.BindSeries(origin.Origin, origin.Kind, origin.TenantID)
	origin.Origin = boundedOrigin
	origin.TenantID = boundedTenant

	outcome := "success"
	if err != nil {
		outcome = "error"
	}
	if operation == "" {
		operation = "store.query"
	}

	DBAttributedQueriesTotal.WithLabelValues(boundedOrigin, origin.Kind, boundedTenant, operation, outcome).Inc()
	DBAttributedQueryDurationSeconds.WithLabelValues(boundedOrigin, origin.Kind, boundedTenant, operation).Observe(duration.Seconds())
	if rows > 0 {
		DBAttributedRowsTotal.WithLabelValues(boundedOrigin, origin.Kind, boundedTenant, operation).Add(float64(rows))
	}

	if l != nil {
		durNs := duration.Nanoseconds()
		if durNs < 0 {
			durNs = 0
		}
		l.mu.Lock()
		if oc := l.origins[boundedOrigin]; oc != nil {
			oc.Queries++
			if err != nil {
				oc.Errors++
			}
			if rows > 0 {
				oc.RowsProcessed += uint64(rows)
			}
			oc.TotalDurationNs += durNs
			if durNs > oc.MaxDurationNs {
				oc.MaxDurationNs = durNs
			}
		}
		if tc := l.tenants[boundedTenant]; tc != nil {
			tc.Queries++
			if err != nil {
				tc.Errors++
			}
			if rows > 0 {
				tc.RowsProcessed += uint64(rows)
			}
			tc.TotalDurationNs += durNs
			if durNs > tc.MaxDurationNs {
				tc.MaxDurationNs = durNs
			}
		}
		l.mu.Unlock()
	}

	return origin, attributed
}

// RecordSlowQuery increments the slow-query counters for the resolved origin and tenant.
func (l *CostAttributionLedger) RecordSlowQuery(ctx context.Context, operation string) QueryOrigin {
	origin, _ := ResolveQueryOrigin(ctx)
	boundedOrigin, boundedTenant := l.BindSeries(origin.Origin, origin.Kind, origin.TenantID)
	origin.Origin = boundedOrigin
	origin.TenantID = boundedTenant
	if operation == "" {
		operation = "store.query"
	}
	DBAttributedSlowQueriesTotal.WithLabelValues(boundedOrigin, origin.Kind, boundedTenant, operation).Inc()

	if l != nil {
		l.mu.Lock()
		if oc := l.origins[boundedOrigin]; oc != nil {
			oc.SlowQueries++
		}
		if tc := l.tenants[boundedTenant]; tc != nil {
			tc.SlowQueries++
		}
		l.mu.Unlock()
	}
	return origin
}

// OriginCost returns the accumulated cost summary for a single bounded origin.
func (l *CostAttributionLedger) OriginCost(origin string) (OriginCostSummary, bool) {
	if l == nil {
		return OriginCostSummary{}, false
	}
	l.mu.RLock()
	defer l.mu.RUnlock()
	oc, ok := l.origins[origin]
	if !ok || oc == nil {
		return OriginCostSummary{}, false
	}
	return *oc, true
}

// TenantCost returns the accumulated cost summary for a single opaque tenant bucket.
func (l *CostAttributionLedger) TenantCost(tenantID string) (TenantCostSummary, bool) {
	if l == nil {
		return TenantCostSummary{}, false
	}
	l.mu.RLock()
	defer l.mu.RUnlock()
	tc, ok := l.tenants[tenantID]
	if !ok || tc == nil {
		return TenantCostSummary{}, false
	}
	return *tc, true
}

// Snapshot returns a deterministic, sorted copy of all origin and tenant cost summaries.
func (l *CostAttributionLedger) Snapshot() CostAttributionSnapshot {
	if l == nil {
		return CostAttributionSnapshot{}
	}
	l.mu.RLock()
	defer l.mu.RUnlock()

	origins := make([]OriginCostSummary, 0, len(l.origins))
	for _, oc := range l.origins {
		origins = append(origins, *oc)
	}
	sort.Slice(origins, func(i, j int) bool {
		if origins[i].TotalDurationNs != origins[j].TotalDurationNs {
			return origins[i].TotalDurationNs > origins[j].TotalDurationNs
		}
		return origins[i].Origin < origins[j].Origin
	})

	tenants := make([]TenantCostSummary, 0, len(l.tenants))
	for _, tc := range l.tenants {
		tenants = append(tenants, *tc)
	}
	sort.Slice(tenants, func(i, j int) bool {
		if tenants[i].TotalDurationNs != tenants[j].TotalDurationNs {
			return tenants[i].TotalDurationNs > tenants[j].TotalDurationNs
		}
		return tenants[i].TenantID < tenants[j].TenantID
	})

	return CostAttributionSnapshot{
		MaxOriginSeries:      l.maxOrigins,
		MaxTenantSeries:      l.maxTenants,
		OriginOverflowEvents: l.originOverflowEvents,
		TenantOverflowEvents: l.tenantOverflowEvents,
		ByOrigin:             origins,
		ByTenant:             tenants,
	}
}

// AttributionLogAttrs returns the structured slog key/value attributes for a
// store query's originator, opaque tenant, and SQL comment tag.
func AttributionLogAttrs(ctx context.Context, operation string) []any {
	origin, _ := ResolveQueryOrigin(ctx)
	boundedOrigin, boundedTenant := DefaultCostLedger().BindSeries(origin.Origin, origin.Kind, origin.TenantID)
	return []any{
		telemetry.FieldOrigin, boundedOrigin,
		telemetry.FieldOriginKind, origin.Kind,
		telemetry.FieldTenantID, boundedTenant,
		telemetry.FieldSQLTag, FormatSQLCommentTag(ctx, operation),
	}
}

// TracingStore decorates a Store with OpenTelemetry spans and bounded database
// cost attribution for persistence and query operations. It preserves the
// underlying store behavior while giving operators both a trace view and an
// attributed cost view of the database boundary.
type TracingStore struct {
	Store
	tracer trace.Tracer
	ledger *CostAttributionLedger
}

// TracingStore is passed to callers as Store, so it must keep satisfying the
// interface as it evolves. This assertion catches a wrapped method whose
// signature drifts from the interface; methods that are still promoted from
// the embedded Store are listed in the tracing coverage note on #1154.
var _ Store = (*TracingStore)(nil)

// NewTracingStore returns a Store wrapper that records spans and cost
// attribution for the common persistence and lookup operations used by the API
// and background workers.
func NewTracingStore(base Store, tracer trace.Tracer) Store {
	if tracer == nil {
		tracer = noop.NewTracerProvider().Tracer("sorotrail/store")
	}
	return &TracingStore{
		Store:  base,
		tracer: tracer,
		ledger: DefaultCostLedger(),
	}
}

// WithCostLedger configures a dedicated CostAttributionLedger on TracingStore
// (used by tests to inspect isolated cost snapshots without global state).
func (s *TracingStore) WithCostLedger(ledger *CostAttributionLedger) *TracingStore {
	if ledger != nil {
		s.ledger = ledger
	}
	return s
}

// CostSnapshot returns a point-in-time snapshot of the store's attributed query costs.
func (s *TracingStore) CostSnapshot() CostAttributionSnapshot {
	if s == nil || s.ledger == nil {
		return DefaultCostLedger().Snapshot()
	}
	return s.ledger.Snapshot()
}

func (s *TracingStore) recordAttribution(ctx context.Context, span trace.Span, operation string, start time.Time, rows int64, err error) {
	ledger := s.ledger
	if ledger == nil {
		ledger = DefaultCostLedger()
	}
	origin, attributed := ledger.RecordQuery(ctx, operation, time.Since(start), rows, err)
	if attributed {
		span.SetAttributes(
			attribute.String("store.origin", origin.Origin),
			attribute.String("store.origin_kind", origin.Kind),
			attribute.String("store.tenant", origin.TenantID),
		)
	}
}

func (s *TracingStore) UpsertEvents(ctx context.Context, events []Event) (int64, error) {
	start := time.Now()
	ctx = context.WithValue(ctx, queryNameContextKey{}, "store.UpsertEvents")
	ctx, span := s.tracer.Start(ctx, "store.UpsertEvents")
	defer span.End()
	span.SetAttributes(attribute.Int("store.event_count", len(events)))
	inserted, err := s.Store.UpsertEvents(ctx, events)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
	s.recordAttribution(ctx, span, "store.UpsertEvents", start, int64(len(events)), err)
	return inserted, err
}

func (s *TracingStore) ReplaceEventsInRange(ctx context.Context, events []Event, fromLedger, toLedger int64) error {
	start := time.Now()
	ctx = context.WithValue(ctx, queryNameContextKey{}, "store.ReplaceEventsInRange")
	ctx, span := s.tracer.Start(ctx, "store.ReplaceEventsInRange")
	defer span.End()
	span.SetAttributes(attribute.Int64("store.from_ledger", fromLedger), attribute.Int64("store.to_ledger", toLedger), attribute.Int("store.event_count", len(events)))
	err := s.Store.ReplaceEventsInRange(ctx, events, fromLedger, toLedger)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
	s.recordAttribution(ctx, span, "store.ReplaceEventsInRange", start, int64(len(events)), err)
	return err
}

func (s *TracingStore) GetEvent(ctx context.Context, id string, sc Scope) (Event, error) {
	start := time.Now()
	ctx = context.WithValue(ctx, queryNameContextKey{}, "store.GetEvent")
	ctx, span := s.tracer.Start(ctx, "store.GetEvent")
	defer span.End()
	span.SetAttributes(attribute.String("store.event_id", id))
	event, err := s.Store.GetEvent(ctx, id, sc)
	rows := int64(1)
	if err != nil {
		rows = 0
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
	s.recordAttribution(ctx, span, "store.GetEvent", start, rows, err)
	return event, err
}

func (s *TracingStore) EventExists(ctx context.Context, id string, sc Scope) (bool, error) {
	start := time.Now()
	ctx = context.WithValue(ctx, queryNameContextKey{}, "store.EventExists")
	ctx, span := s.tracer.Start(ctx, "store.EventExists")
	defer span.End()
	span.SetAttributes(attribute.String("store.event_id", id))
	exists, err := s.Store.EventExists(ctx, id, sc)
	rows := int64(0)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	} else if exists {
		rows = 1
	}
	s.recordAttribution(ctx, span, "store.EventExists", start, rows, err)
	return exists, err
}

func (s *TracingStore) QueryEvents(ctx context.Context, f EventFilter) ([]Event, string, error) {
	start := time.Now()
	ctx = context.WithValue(ctx, queryNameContextKey{}, "store.QueryEvents")
	ctx, span := s.tracer.Start(ctx, "store.QueryEvents")
	defer span.End()
	span.SetAttributes(attribute.String("store.contract_id", f.ContractID), attribute.String("store.types", fmt.Sprint(f.Types)))
	events, cursor, err := s.Store.QueryEvents(ctx, f)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	} else {
		span.SetAttributes(attribute.Int("store.event_count", len(events)))
	}
	s.recordAttribution(ctx, span, "store.QueryEvents", start, int64(len(events)), err)
	return events, cursor, err
}

func (s *TracingStore) GetIngestionState(ctx context.Context) (IngestionState, error) {
	start := time.Now()
	ctx = context.WithValue(ctx, queryNameContextKey{}, "store.GetIngestionState")
	ctx, span := s.tracer.Start(ctx, "store.GetIngestionState")
	defer span.End()
	state, err := s.Store.GetIngestionState(ctx)
	rows := int64(1)
	if err != nil {
		rows = 0
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
	s.recordAttribution(ctx, span, "store.GetIngestionState", start, rows, err)
	return state, err
}

func (s *TracingStore) GetIngestionStateForNetwork(ctx context.Context, network string) (IngestionState, error) {
	start := time.Now()
	ctx = context.WithValue(ctx, queryNameContextKey{}, "store.GetIngestionStateForNetwork")
	ctx, span := s.tracer.Start(ctx, "store.GetIngestionStateForNetwork")
	defer span.End()
	span.SetAttributes(attribute.String("store.network", network))
	if r, ok := s.Store.(interface {
		GetIngestionStateForNetwork(context.Context, string) (IngestionState, error)
	}); ok {
		state, err := r.GetIngestionStateForNetwork(ctx, network)
		rows := int64(1)
		if err != nil {
			rows = 0
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
		s.recordAttribution(ctx, span, "store.GetIngestionStateForNetwork", start, rows, err)
		return state, err
	}
	if defaultNetwork(network) == "default" {
		state, err := s.Store.GetIngestionState(ctx)
		rows := int64(1)
		if err != nil {
			rows = 0
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
		s.recordAttribution(ctx, span, "store.GetIngestionStateForNetwork", start, rows, err)
		return state, err
	}
	// A store without the optional method only knows the default network, so
	// this is a real lookup failure and belongs on the span like any other.
	span.RecordError(ErrNotFound)
	span.SetStatus(codes.Error, ErrNotFound.Error())
	s.recordAttribution(ctx, span, "store.GetIngestionStateForNetwork", start, 0, ErrNotFound)
	return IngestionState{}, ErrNotFound
}

func (s *TracingStore) SaveIngestionState(ctx context.Context, state IngestionState) error {
	start := time.Now()
	ctx = context.WithValue(ctx, queryNameContextKey{}, "store.SaveIngestionState")
	ctx, span := s.tracer.Start(ctx, "store.SaveIngestionState")
	defer span.End()
	span.SetAttributes(attribute.Int64("store.last_ingested_ledger", state.LastIngestedLedger))
	err := s.Store.SaveIngestionState(ctx, state)
	rows := int64(1)
	if err != nil {
		rows = 0
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
	s.recordAttribution(ctx, span, "store.SaveIngestionState", start, rows, err)
	return err
}
