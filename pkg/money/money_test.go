package money

import (
	"errors"
	"math"
	"testing"
)

func TestAmountArithmeticAndOverflow(t *testing.T) {
	a := FromInt64(1001)
	b := FromInt64(59)

	sum, err := a.Add(b)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if sum.String() != "1060" {
		t.Fatalf("expected 1060, got %s", sum.String())
	}

	diff, err := a.Sub(b)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if diff.String() != "942" {
		t.Fatalf("expected 942, got %s", diff.String())
	}

	// Overflow checks
	if _, err := MaxAmount.Add(FromInt64(1)); !errors.Is(err, ErrOverflow) {
		t.Fatalf("expected ErrOverflow on MaxAmount + 1, got %v", err)
	}
	if _, err := MinAmount.Sub(FromInt64(1)); !errors.Is(err, ErrOverflow) {
		t.Fatalf("expected ErrOverflow on MinAmount - 1, got %v", err)
	}
	if _, err := MinAmount.Neg(); !errors.Is(err, ErrOverflow) {
		t.Fatalf("expected ErrOverflow on -MinAmount, got %v", err)
	}

	// Large int128 beyond int64
	high := FromParts(0, math.MaxUint64)
	doubled, err := high.Add(high)
	if err != nil {
		t.Fatalf("unexpected error adding MaxUint64: %v", err)
	}
	parsed, err := ParseMinor(doubled.String())
	if err != nil || parsed.Cmp(doubled) != 0 {
		t.Fatalf("round-trip failed for %s: %v", doubled.String(), err)
	}
}

func TestParseAndFormat(t *testing.T) {
	usd := Asset{Code: "USD", Scale: 2}
	jpy := Asset{Code: "JPY", Scale: 0}
	eth := Asset{Code: "ETH", Scale: 18}

	amt, err := Parse("10.01", usd)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if amt.String() != "1001" {
		t.Fatalf("expected 1001, got %s", amt.String())
	}
	if got := Format(amt, usd); got != "10.01" {
		t.Fatalf("expected 10.01, got %s", got)
	}

	// Extra precision must be rejected
	if _, err := Parse("10.015", usd); !errors.Is(err, ErrExtraPrecision) {
		t.Fatalf("expected ErrExtraPrecision for 10.015 USD/2, got %v", err)
	}

	// JPY/0 rejects any decimal point
	if _, err := Parse("100.0", jpy); !errors.Is(err, ErrExtraPrecision) {
		t.Fatalf("expected ErrExtraPrecision for 100.0 JPY/0, got %v", err)
	}
	jpyAmt, err := Parse("500", jpy)
	if err != nil || Format(jpyAmt, jpy) != "500" {
		t.Fatalf("unexpected JPY parse/format result: %v, %s", err, Format(jpyAmt, jpy))
	}

	// ETH/18 large amount
	ethAmt, err := Parse("123.456789012345678901", eth)
	if err != nil {
		t.Fatalf("ETH parse failed: %v", err)
	}
	if got := Format(ethAmt, eth); got != "123.456789012345678901" {
		t.Fatalf("ETH format mismatch: %s", got)
	}
}

func TestMulRateWorkedExamples(t *testing.T) {
	// RFC Section 7 worked example: 2.9% fee on 10.01 USD (1001 minor units)
	x := FromInt64(1001)
	r := Rate{Mantissa: FromInt64(29), Scale: 3} // 2.9%

	res, rem, err := MulRate(x, r, HalfEven)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.Cmp(FromInt64(29)) != 0 {
		t.Fatalf("expected 29 minor units, got %s", res)
	}
	if rem.Cmp(FromInt64(29)) != 0 {
		t.Fatalf("expected remainder 29 (0.029 minor units), got %s", rem)
	}

	// HalfEven vs HalfUp on tie (2.5 -> 2 for HalfEven, 3 for HalfUp)
	tieRate := Rate{Mantissa: FromInt64(25), Scale: 1} // 2.5
	evenRes, _, _ := MulRate(FromInt64(1), tieRate, HalfEven)
	upRes, _, _ := MulRate(FromInt64(1), tieRate, HalfUp)
	if evenRes.Cmp(FromInt64(2)) != 0 {
		t.Fatalf("expected HalfEven(2.5) = 2, got %s", evenRes)
	}
	if upRes.Cmp(FromInt64(3)) != 0 {
		t.Fatalf("expected HalfUp(2.5) = 3, got %s", upRes)
	}
}

func TestRescaleInterestAccrualExample(t *testing.T) {
	// RFC Section 7 worked example: 1.234567 USD/6 -> USD/2
	accrued := FromInt64(1_234_567)
	payable, rem, err := Rescale(accrued, 6, 2, HalfEven)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if payable.Cmp(FromInt64(123)) != 0 {
		t.Fatalf("expected 123 (1.23 USD/2), got %s", payable)
	}
	if rem.Cmp(FromInt64(4567)) != 0 {
		t.Fatalf("expected remainder 4567 (0.004567 USD/6), got %s", rem)
	}
}

func TestAllocateThreeWaySplit(t *testing.T) {
	// RFC Section 7 worked example: 100.00 USD (10000) split three ways -> 3334, 3333, 3333
	parts, err := Allocate(FromInt64(10_000), []uint64{1, 1, 1})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(parts) != 3 {
		t.Fatalf("expected 3 parts, got %d", len(parts))
	}
	if parts[0].Cmp(FromInt64(3334)) != 0 || parts[1].Cmp(FromInt64(3333)) != 0 || parts[2].Cmp(FromInt64(3333)) != 0 {
		t.Fatalf("unexpected parts: %v", parts)
	}
}

func FuzzParseFormatRoundTrip(f *testing.F) {
	f.Add(int64(0), uint8(2))
	f.Add(int64(1001), uint8(2))
	f.Add(int64(-999999), uint8(6))
	f.Add(int64(123456789), uint8(0))
	f.Add(int64(42), uint8(18))

	f.Fuzz(func(t *testing.T, val int64, scale uint8) {
		scale = scale % (MaxScale + 1)
		asset := Asset{Code: "TST", Scale: scale}
		amt := FromInt64(val)
		formatted := Format(amt, asset)
		parsed, err := Parse(formatted, asset)
		if err != nil {
			t.Fatalf("failed to parse %q (scale %d): %v", formatted, scale, err)
		}
		if parsed.Cmp(amt) != 0 {
			t.Fatalf("round-trip mismatch: %s -> %q -> %s", amt, formatted, parsed)
		}
	})
}

func FuzzAllocateConservation(f *testing.F) {
	f.Add(int64(10000), uint64(1), uint64(1), uint64(1))
	f.Add(int64(1), uint64(1), uint64(2), uint64(3))
	f.Add(int64(9999999), uint64(33), uint64(33), uint64(34))

	f.Fuzz(func(t *testing.T, total int64, w1, w2, w3 uint64) {
		if total < 0 {
			total = -total
		}
		// keep weights bounded to avoid uint64 sum overflow in test input
		w1 %= 1_000_000
		w2 %= 1_000_000
		w3 %= 1_000_000
		if w1 == 0 && w2 == 0 && w3 == 0 {
			w1 = 1
		}
		totAmt := FromInt64(total)
		parts, err := Allocate(totAmt, []uint64{w1, w2, w3})
		if err != nil {
			t.Fatalf("Allocate failed: %v", err)
		}
		sum := Zero
		for _, p := range parts {
			if p.IsNegative() {
				t.Fatalf("negative part %s for non-negative total %s", p, totAmt)
			}
			sum, err = sum.Add(p)
			if err != nil {
				t.Fatalf("sum overflow: %v", err)
			}
		}
		if sum.Cmp(totAmt) != 0 {
			t.Fatalf("parts sum %s != total %s", sum, totAmt)
		}
	})
}
