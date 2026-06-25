package transaction

import "errors"

// ErrIllegalTransition is returned by Apply for any (from,event) pair that is
// not in the legal transition table.
var ErrIllegalTransition = errors.New("illegal transition")

// transitionKey is the lookup key for the legal-transition table.
type transitionKey struct {
	from Status
	ev   Event
}

// transitions encodes the full legal-transition table (v1 doc §4). The zero
// Status ("") is the bootstrap state from which only Authorize is legal.
var transitions = map[transitionKey]Status{
	{Status(""), EventAuthorize}:               StatusAuthorizing,
	{StatusAuthorizing, EventAcquirerApproved}: StatusAuthorized,
	{StatusAuthorizing, EventAcquirerDeclined}: StatusDeclined,
	{StatusAuthorized, EventStartDispense}:     StatusDispensing,
	{StatusAuthorized, EventHoldTimeout}:       StatusExpired,
	{StatusAuthorized, EventCancel}:            StatusVoided,
	{StatusDispensing, EventPumpStopped}:       StatusCompleted,
	{StatusCompleted, EventCapture}:            StatusCapturing,
	{StatusCapturing, EventAcquirerCaptured}:   StatusCaptured,
	{StatusCaptured, EventIncludeInBatch}:      StatusSettled,
	{StatusCaptured, EventReverse}:             StatusReversed,
}

// Apply returns the destination status for a legal (from,event) pair, or
// ErrIllegalTransition. It is a pure function over the transition table;
// amount guards (e.g. captured <= auth) are enforced at the store/service
// layer, not here. On error it returns the zero Status.
func Apply(from Status, e Event) (Status, error) {
	to, ok := transitions[transitionKey{from: from, ev: e}]
	if !ok {
		return Status(""), ErrIllegalTransition
	}
	return to, nil
}
