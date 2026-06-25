package transaction

// Status is the lifecycle state of a fuel transaction aggregate.
type Status string

const (
	StatusAuthorizing Status = "AUTHORIZING"
	StatusAuthorized  Status = "AUTHORIZED"
	StatusDispensing  Status = "DISPENSING"
	StatusCompleted   Status = "COMPLETED"
	StatusCapturing   Status = "CAPTURING"
	StatusCaptured    Status = "CAPTURED"
	StatusSettled     Status = "SETTLED"
	StatusDeclined    Status = "DECLINED"
	StatusVoided      Status = "VOIDED"
	StatusExpired     Status = "EXPIRED"
	StatusReversed    Status = "REVERSED"
	StatusFailed      Status = "FAILED"
)

// IsTerminal reports whether the status is one of the terminal set
// (SETTLED, DECLINED, VOIDED, EXPIRED, REVERSED, FAILED). This is the exact
// predicate negated by the one_active_txn_per_pump partial unique index
// (ADR-004): a row counts as "live" for the per-pump uniqueness constraint
// iff its status is NOT terminal.
func (s Status) IsTerminal() bool {
	switch s {
	case StatusSettled, StatusDeclined, StatusVoided,
		StatusExpired, StatusReversed, StatusFailed:
		return true
	default:
		return false
	}
}
