package money

import "testing"

func TestAdd(t *testing.T) {
	cases := []struct {
		name     string
		a, b, want Amount
	}{
		{"positives", 100, 25, 125},
		{"add zero", 100, 0, 100},
		{"negative result", 100, -250, -150},
		{"two negatives", -100, -25, -125},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := c.a.Add(c.b); got != c.want {
				t.Fatalf("%d.Add(%d) = %d, want %d", c.a, c.b, got, c.want)
			}
		})
	}
}

func TestSub(t *testing.T) {
	cases := []struct {
		name       string
		a, b, want Amount
	}{
		{"positives", 100, 25, 75},
		{"sub zero", 100, 0, 100},
		{"goes negative", 25, 100, -75},
		{"double negative", -100, -25, -75},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := c.a.Sub(c.b); got != c.want {
				t.Fatalf("%d.Sub(%d) = %d, want %d", c.a, c.b, got, c.want)
			}
		})
	}
}

func TestNeg(t *testing.T) {
	cases := []struct {
		in, want Amount
	}{
		{100, -100},
		{-100, 100},
		{0, 0},
	}
	for _, c := range cases {
		if got := c.in.Neg(); got != c.want {
			t.Fatalf("%d.Neg() = %d, want %d", c.in, got, c.want)
		}
	}
}

func TestIsZero(t *testing.T) {
	if !Amount(0).IsZero() {
		t.Fatal("Amount(0).IsZero() = false, want true")
	}
	if Amount(1).IsZero() {
		t.Fatal("Amount(1).IsZero() = true, want false")
	}
	if Amount(-1).IsZero() {
		t.Fatal("Amount(-1).IsZero() = true, want false")
	}
}

func TestString(t *testing.T) {
	cases := []struct {
		in   Amount
		want string
	}{
		{1234, "RM12.34"},
		{100, "RM1.00"},
		{0, "RM0.00"},
		{9, "RM0.09"},
		{99, "RM0.99"},
		{5, "RM0.05"},
		{-5, "-RM0.05"},
		{-150, "-RM1.50"},
		{-1234, "-RM12.34"},
		{-9, "-RM0.09"},
		{1000000, "RM10000.00"},
	}
	for _, c := range cases {
		if got := c.in.String(); got != c.want {
			t.Fatalf("Amount(%d).String() = %q, want %q", int64(c.in), got, c.want)
		}
	}
}
