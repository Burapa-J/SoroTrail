package telemetry

import (
	"log/slog"
	"net/url"
	"strings"
)

// Canonical log field names across all SoroTrail packages.
//
// By standardizing these keys, queries and aggregations across the pipeline
// remain consistent and reliable.
const (
	// FieldContractID is the canonical key for Stellar/Soroban contract identifiers.
	FieldContractID = "contract_id"

	// FieldLedger is the canonical key for ledger sequence numbers.
	FieldLedger = "ledger"

	// FieldDurationMs is the canonical key for operation durations expressed in milliseconds.
	FieldDurationMs = "duration_ms"

	// FieldError is the canonical key for error values.
	FieldError = "error"

	// FieldURL is the canonical key for target URLs.
	FieldURL = "url"

	// FieldTxHash is the canonical key for transaction hashes.
	FieldTxHash = "tx_hash"

	// FieldOrigin is the canonical key for the bounded query originator
	// (an HTTP route pattern such as "GET /events" or a background job id
	// such as "ingester", "pruner", "archiver", "reconciler", "auditor", "webhook").
	FieldOrigin = "origin"

	// FieldOriginKind is the canonical key for the originator class ("route" | "job" | "unattributed").
	FieldOriginKind = "origin_kind"

	// FieldTenantID is the canonical key for the opaque tenant identifier
	// ("system", "untenanted", "t_<id>", or "t_other").
	FieldTenantID = "tenant_id"

	// FieldQueryOp is the canonical key for the store operation name (e.g. "store.QueryEvents").
	FieldQueryOp = "query_op"

	// FieldSQLTag is the canonical key for the normalized SQL comment tag attached to a statement.
	FieldSQLTag = "sql_tag"
)

// SanitizeURL removes basic-auth credentials from a URL string so that secrets
// never reach a log line or telemetry sink.
func SanitizeURL(rawURL string) string {
	parsed, err := url.Parse(rawURL)
	if err != nil {
		// Nothing here can tell a password from the rest of an unparseable
		// string, and returning it unchanged is exactly how a credential
		// reaches the log line this function exists to keep it out of. Note
		// that a valid password is enough to fail url.Parse: a "%", a space or
		// a "[" in the userinfo all do it.
		return "<redacted>"
	}
	if parsed.User != nil {
		if _, passwordSet := parsed.User.Password(); passwordSet {
			parsed.User = url.UserPassword(parsed.User.Username(), "***")
		}
	}
	return parsed.String()
}

// LogValueSanitized returns a slog.Attr with credentials redacted if the value looks like a URL.
func LogValueSanitized(key, val string) slog.Attr {
	if strings.Contains(val, "://") {
		return slog.String(key, SanitizeURL(val))
	}
	return slog.String(key, val)
}

// WithContractID returns a slog.Attr for the canonical contract_id field.
func WithContractID(contractID string) slog.Attr {
	return slog.String(FieldContractID, contractID)
}

// WithLedger returns a slog.Attr for the canonical ledger field.
func WithLedger(ledger int64) slog.Attr {
	return slog.Int64(FieldLedger, ledger)
}

// WithDurationMs returns a slog.Attr for the canonical duration_ms field.
func WithDurationMs(ms int64) slog.Attr {
	return slog.Int64(FieldDurationMs, ms)
}

// WithError returns a slog.Attr for the canonical error field.
func WithError(err error) slog.Attr {
	if err == nil {
		return slog.String(FieldError, "")
	}
	return slog.String(FieldError, err.Error())
}

// WithOrigin returns a slog.Attr for the canonical origin field.
func WithOrigin(origin string) slog.Attr {
	return slog.String(FieldOrigin, origin)
}

// WithTenantID returns a slog.Attr for the canonical opaque tenant_id field.
func WithTenantID(tenantID string) slog.Attr {
	return slog.String(FieldTenantID, tenantID)
}

// WithSQLTag returns a slog.Attr for the canonical sql_tag field.
func WithSQLTag(tag string) slog.Attr {
	return slog.String(FieldSQLTag, tag)
}
