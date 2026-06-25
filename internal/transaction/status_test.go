package transaction

import "testing"

func TestStatusIsTerminal(t *testing.T) {
	terminal := []Status{
		StatusSettled, StatusDeclined, StatusVoided,
		StatusExpired, StatusReversed, StatusFailed,
	}
	nonTerminal := []Status{
		StatusAuthorizing, StatusAuthorized, StatusDispensing,
		StatusCompleted, StatusCapturing, StatusCaptured,
	}
	for _, s := range terminal {
		if !s.IsTerminal() {
			t.Errorf("Status(%q).IsTerminal() = false, want true", s)
		}
	}
	for _, s := range nonTerminal {
		if s.IsTerminal() {
			t.Errorf("Status(%q).IsTerminal() = true, want false", s)
		}
	}
}

func TestStatusValues(t *testing.T) {
	cases := map[Status]string{
		StatusAuthorizing: "AUTHORIZING",
		StatusAuthorized:  "AUTHORIZED",
		StatusDispensing:  "DISPENSING",
		StatusCompleted:   "COMPLETED",
		StatusCapturing:   "CAPTURING",
		StatusCaptured:    "CAPTURED",
		StatusSettled:     "SETTLED",
		StatusDeclined:    "DECLINED",
		StatusVoided:      "VOIDED",
		StatusExpired:     "EXPIRED",
		StatusReversed:    "REVERSED",
		StatusFailed:      "FAILED",
	}
	for got, want := range cases {
		if string(got) != want {
			t.Errorf("status const = %q, want %q", string(got), want)
		}
	}
}
