package transaction

// Event is an input that may drive the transaction state machine.
type Event string

const (
	EventAuthorize        Event = "Authorize"
	EventAcquirerApproved Event = "AcquirerApproved"
	EventAcquirerDeclined Event = "AcquirerDeclined"
	EventStartDispense    Event = "StartDispense"
	EventPumpStopped      Event = "PumpStopped"
	EventCapture          Event = "Capture"
	EventAcquirerCaptured Event = "AcquirerCaptured"
	EventHoldTimeout      Event = "HoldTimeout"
	EventCancel           Event = "Cancel"
	EventReverse          Event = "Reverse"
	EventIncludeInBatch   Event = "IncludeInBatch"
)
