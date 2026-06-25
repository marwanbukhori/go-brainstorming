package transaction

import (
	"errors"
	"testing"
)

func TestEventValues(t *testing.T) {
	cases := map[Event]string{
		EventAuthorize:        "Authorize",
		EventAcquirerApproved: "AcquirerApproved",
		EventAcquirerDeclined: "AcquirerDeclined",
		EventStartDispense:    "StartDispense",
		EventPumpStopped:      "PumpStopped",
		EventCapture:          "Capture",
		EventAcquirerCaptured: "AcquirerCaptured",
		EventHoldTimeout:      "HoldTimeout",
		EventCancel:           "Cancel",
		EventReverse:          "Reverse",
		EventIncludeInBatch:   "IncludeInBatch",
	}
	for got, want := range cases {
		if string(got) != want {
			t.Errorf("event const = %q, want %q", string(got), want)
		}
	}
}

func TestApplyLegalTransitions(t *testing.T) {
	legal := []struct {
		from Status
		ev   Event
		to   Status
	}{
		{Status(""), EventAuthorize, StatusAuthorizing},
		{StatusAuthorizing, EventAcquirerApproved, StatusAuthorized},
		{StatusAuthorizing, EventAcquirerDeclined, StatusDeclined},
		{StatusAuthorized, EventStartDispense, StatusDispensing},
		{StatusAuthorized, EventHoldTimeout, StatusExpired},
		{StatusAuthorized, EventCancel, StatusVoided},
		{StatusDispensing, EventPumpStopped, StatusCompleted},
		{StatusCompleted, EventCapture, StatusCapturing},
		{StatusCapturing, EventAcquirerCaptured, StatusCaptured},
		{StatusCaptured, EventIncludeInBatch, StatusSettled},
		{StatusCaptured, EventReverse, StatusReversed},
	}
	for _, c := range legal {
		got, err := Apply(c.from, c.ev)
		if err != nil {
			t.Errorf("Apply(%q,%q) returned err %v, want nil", c.from, c.ev, err)
			continue
		}
		if got != c.to {
			t.Errorf("Apply(%q,%q) = %q, want %q", c.from, c.ev, got, c.to)
		}
	}
}

func TestApplyIllegalTransitions(t *testing.T) {
	illegal := []struct {
		from Status
		ev   Event
	}{
		{Status(""), EventCapture},               // bootstrap only accepts Authorize
		{StatusAuthorizing, EventStartDispense},  // must be approved first
		{StatusAuthorized, EventCapture},         // must dispense+complete first
		{StatusDispensing, EventCapture},         // must stop the pump first
		{StatusCompleted, EventAcquirerCaptured}, // must request Capture first
		{StatusCaptured, EventAuthorize},         // already past authorize
		{StatusSettled, EventReverse},            // terminal, no transitions out
		{StatusDeclined, EventAuthorize},         // terminal
		{StatusVoided, EventCapture},             // terminal
		{StatusExpired, EventStartDispense},      // terminal
		{StatusReversed, EventCapture},           // terminal
		{StatusFailed, EventAuthorize},           // terminal
	}
	for _, c := range illegal {
		got, err := Apply(c.from, c.ev)
		if !errors.Is(err, ErrIllegalTransition) {
			t.Errorf("Apply(%q,%q) err = %v, want ErrIllegalTransition", c.from, c.ev, err)
		}
		if got != Status("") {
			t.Errorf("Apply(%q,%q) status = %q, want zero on error", c.from, c.ev, got)
		}
	}
}
