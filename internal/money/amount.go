// Package money holds the float-free monetary type for the fuel-POS core.
// Amount is integer minor units (sen); 100 == RM1.00. No floats, ever
// (spec §8 / money rules).
package money

// Amount is money in integer minor units (sen). 100 = RM1.00.
type Amount int64

// Add returns a + b.
func (a Amount) Add(b Amount) Amount { return a + b }

// Sub returns a - b.
func (a Amount) Sub(b Amount) Amount { return a - b }

// Neg returns -a.
func (a Amount) Neg() Amount { return -a }

// IsZero reports whether a == 0.
func (a Amount) IsZero() bool { return a == 0 }
