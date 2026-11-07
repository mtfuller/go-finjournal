package money

import (
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"math"
	"math/big"
	"os"
	"path/filepath"
	"strings"
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

	// Large int128 beyond int64 (both positive and negative)
	high := FromParts(0, math.MaxUint64)
	doubled, err := high.Add(high)
	if err != nil {
		t.Fatalf("unexpected error adding MaxUint64: %v", err)
	}
	parsed, err := ParseMinor(doubled.String())
	if err != nil || parsed.Cmp(doubled) != 0 {
		t.Fatalf("round-trip failed for %s: %v", doubled.String(), err)
	}

	negDoubled, err := doubled.Neg()
	if err != nil {
		t.Fatalf("unexpected error negating doubled: %v", err)
	}
	parsedNeg, err := ParseMinor(negDoubled.String())
	if err != nil || parsedNeg.Cmp(negDoubled) != 0 {
		t.Fatalf("negative round-trip failed for %s: got %s (err=%v)", negDoubled.String(), parsedNeg.String(), err)
	}

	// MaxAmount and MinAmount string round-trip
	maxParsed, err := ParseMinor(MaxAmount.String())
	if err != nil || maxParsed.Cmp(MaxAmount) != 0 {
		t.Fatalf("MaxAmount round-trip failed: %v", err)
	}
	minParsed, err := ParseMinor(MinAmount.String())
	if err != nil || minParsed.Cmp(MinAmount) != 0 {
		t.Fatalf("MinAmount round-trip failed: %v", err)
	}
}

func TestZeroHeapAllocationsOnHotPath(t *testing.T) {
	x := FromInt64(100_100)
	y := FromInt64(5_900)
	r := Rate{Mantissa: FromInt64(29), Scale: 3}

	allocsAdd := testing.AllocsPerRun(100, func() {
		_, _ = x.Add(y)
		_, _ = x.Sub(y)
	})
	if allocsAdd != 0 {
		t.Fatalf("expected 0 allocations for Add/Sub, got %v", allocsAdd)
	}

	allocsMul := testing.AllocsPerRun(100, func() {
		_, _, _ = MulRate(x, r, HalfEven)
	})
	if allocsMul != 0 {
		t.Fatalf("expected 0 allocations for MulRate, got %v", allocsMul)
	}

	allocsRescale := testing.AllocsPerRun(100, func() {
		_, _, _ = Rescale(x, 6, 2, HalfEven)
		_, _, _ = Rescale(x, 2, 6, HalfEven)
		// Also test scale > 19 (256-by-128 path)
		_, _, _ = Rescale(x, 25, 2, HalfEven)
	})
	if allocsRescale != 0 {
		t.Fatalf("expected 0 allocations for Rescale, got %v", allocsRescale)
	}
}

func TestNoFloatsInMoneyAndLedgerPackages(t *testing.T) {
	// RFC Section 7: "Floats are banned from pkg/money and pkg/ledger by a forbidigo lint rule."
	dirs := []string{".", "../ledger", "../core"}
	fset := token.NewFileSet()

	for _, dir := range dirs {
		entries, err := os.ReadDir(dir)
		if err != nil {
			t.Fatalf("ReadDir(%s): %v", dir, err)
		}
		for _, entry := range entries {
			name := entry.Name()
			if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
				continue
			}
			fullPath := filepath.Join(dir, name)
			f, err := parser.ParseFile(fset, fullPath, nil, 0)
			if err != nil {
				t.Fatalf("ParseFile(%s): %v", fullPath, err)
			}
			ast.Inspect(f, func(n ast.Node) bool {
				switch node := n.(type) {
				case *ast.Ident:
					if node.Name == "float32" || node.Name == "float64" {
						t.Errorf("forbidden floating-point type %q in %s:%d",
							node.Name, fullPath, fset.Position(node.Pos()).Line)
					}
				case *ast.BasicLit:
					if node.Kind == token.FLOAT {
						t.Errorf("forbidden floating-point literal %q in %s:%d",
							node.Value, fullPath, fset.Position(node.Pos()).Line)
					}
				}
				return true
			})
		}
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

func TestCompleteMoneyCoverage(t *testing.T) {
	// 1. FromUint64, Parts, IsPositive, Sign, Abs, Int64, FromBigInt
	u := FromUint64(12345)
	hi, lo := u.Parts()
	if hi != 0 || lo != 12345 || !u.IsPositive() || u.Sign() != 1 {
		t.Fatalf("unexpected FromUint64/Parts/IsPositive/Sign: hi=%d lo=%d", hi, lo)
	}
	if Zero.Sign() != 0 || Zero.IsPositive() {
		t.Fatalf("unexpected Zero Sign/IsPositive")
	}
	neg := FromInt64(-42)
	if neg.Sign() != -1 || neg.IsPositive() {
		t.Fatalf("unexpected negative Sign/IsPositive")
	}
	absVal, err := neg.Abs()
	if err != nil || absVal.Cmp(FromInt64(42)) != 0 {
		t.Fatalf("Abs(-42) = %s, %v", absVal, err)
	}
	posAbs, err := u.Abs()
	if err != nil || posAbs.Cmp(u) != 0 {
		t.Fatalf("Abs(12345) = %s, %v", posAbs, err)
	}
	if _, err := MinAmount.Abs(); !errors.Is(err, ErrOverflow) {
		t.Fatalf("expected MinAmount.Abs() overflow, got %v", err)
	}
	if v, ok := neg.Int64(); !ok || v != -42 {
		t.Fatalf("expected Int64(-42), got %d, %v", v, ok)
	}
	if _, ok := MaxAmount.Int64(); ok {
		t.Fatalf("expected MaxAmount.Int64() to return false")
	}
	if _, ok := MinAmount.Int64(); ok {
		t.Fatalf("expected MinAmount.Int64() to return false")
	}

	// Cmp with different high words and low words
	pBig1 := FromParts(2, 10)
	pBig2 := FromParts(2, 20)
	if pBig1.Cmp(pBig2) != -1 || pBig2.Cmp(pBig1) != 1 || pBig1.Cmp(pBig1) != 0 {
		t.Fatalf("unexpected Cmp on multi-word Amount")
	}

	// FromBigInt round-trips and overflow
	for _, a := range []Amount{Zero, FromInt64(1), FromInt64(-1), MaxAmount, MinAmount, FromParts(5, 999)} {
		bi := a.BigInt()
		back, err := FromBigInt(bi)
		if err != nil || back.Cmp(a) != 0 {
			t.Fatalf("FromBigInt(%s) = %s, %v", a, back, err)
		}
		// Also test String() -> ParseMinor() on multi-word 128-bit values
		s := a.String()
		pm, err := ParseMinor(s)
		if err != nil || pm.Cmp(a) != 0 {
			t.Fatalf("ParseMinor(%q) = %s, %v", s, pm, err)
		}
	}
	tooBig := new(big.Int).Add(MaxAmount.BigInt(), big.NewInt(1))
	if _, err := FromBigInt(tooBig); !errors.Is(err, ErrOverflow) {
		t.Fatalf("expected FromBigInt overflow on > MaxAmount")
	}
	tooSmall := new(big.Int).Sub(MinAmount.BigInt(), big.NewInt(1))
	if _, err := FromBigInt(tooSmall); !errors.Is(err, ErrOverflow) {
		t.Fatalf("expected FromBigInt overflow on < MinAmount")
	}
	if z, err := FromBigInt(nil); err != nil || !z.IsZero() {
		t.Fatalf("expected FromBigInt(nil) == 0")
	}

	// JSON Marshal / Unmarshal
	jb, err := u.MarshalJSON()
	if err != nil || string(jb) != `"12345"` {
		t.Fatalf("MarshalJSON: %s, %v", string(jb), err)
	}
	var unmarshaled Amount
	if err := unmarshaled.UnmarshalJSON([]byte(`"12345"`)); err != nil || unmarshaled.Cmp(u) != 0 {
		t.Fatalf("UnmarshalJSON string: %s, %v", unmarshaled, err)
	}
	if err := unmarshaled.UnmarshalJSON([]byte(`6789`)); err == nil {
		t.Fatalf("expected UnmarshalJSON error on non-string JSON number")
	}
	if err := unmarshaled.UnmarshalJSON([]byte(`"bad"`)); err == nil {
		t.Fatalf("expected UnmarshalJSON error on invalid string")
	}

	// ParseMinor edge cases (+ sign, empty, invalid chars, >18 digits invalid/overflow)
	if pm, err := ParseMinor("+456"); err != nil || pm.Cmp(FromInt64(456)) != 0 {
		t.Fatalf("ParseMinor(+456): %s, %v", pm, err)
	}
	for _, bad := range []string{"", "-", "+", "12a3", "12345678901234567890a", "99999999999999999999999999999999999999999"} {
		if _, err := ParseMinor(bad); err == nil {
			t.Fatalf("expected ParseMinor(%q) error", bad)
		}
	}

	// Parse edge cases
	usd := Asset{Code: "USD", Scale: 2}
	if p, err := Parse("+12.34", usd); err != nil || p.Cmp(FromInt64(1234)) != 0 {
		t.Fatalf("Parse(+12.34): %s, %v", p, err)
	}
	if p, err := Parse("-0.05", usd); err != nil || p.Cmp(FromInt64(-5)) != 0 || Format(p, usd) != "-0.05" {
		t.Fatalf("Parse/Format(-0.05): %s, %v", p, err)
	}
	for _, bad := range []string{"", "-", "+", "1.2.3", ".5", "5.", "1a.00", "1.0a"} {
		if _, err := Parse(bad, usd); err == nil {
			t.Fatalf("expected Parse(%q) error", bad)
		}
	}
	if _, err := Parse("1.00", Asset{Code: "", Scale: 2}); err == nil {
		t.Fatalf("expected Parse error on invalid asset")
	}

	// Asset Validate & ParseAsset
	a, err := ParseAsset("USD/2")
	if err != nil || a.ID() != "USD/2" {
		t.Fatalf("ParseAsset(USD/2): %+v, %v", a, err)
	}
	for _, badAsset := range []AssetID{"INVALID", "USD/abc", "USD/99", "/2", "USD/"} {
		if _, err := ParseAsset(badAsset); err == nil {
			t.Fatalf("expected ParseAsset(%q) error", badAsset)
		}
	}

	// ParseRate & Rate.String
	r1, err := ParseRate("0.029")
	if err != nil || r1.String() != "0.029" {
		t.Fatalf("ParseRate(0.029): %+v (%s), %v", r1, r1.String(), err)
	}
	r2, err := ParseRate("+100")
	if err != nil || r2.String() != "100" {
		t.Fatalf("ParseRate(+100): %+v (%s), %v", r2, r2.String(), err)
	}
	r3, err := ParseRate("-1.25")
	if err != nil || r3.String() != "-1.25" {
		t.Fatalf("ParseRate(-1.25): %+v (%s), %v", r3, r3.String(), err)
	}
	for _, badRate := range []string{"", "-", "+", "1.2.3", ".5", "5.", "1.2345678901234567890123456789012", "1.a"} {
		if _, err := ParseRate(badRate); err == nil {
			t.Fatalf("expected ParseRate(%q) error", badRate)
		}
	}
	if _, _, err := MulRate(FromInt64(10), Rate{Mantissa: FromInt64(1), Scale: 31}, HalfEven); err == nil {
		t.Fatalf("expected MulRate scale > 30 error")
	}
	if _, _, err := MulRate(MaxAmount, Rate{Mantissa: FromInt64(100), Scale: 0}, HalfEven); !errors.Is(err, ErrOverflow) {
		t.Fatalf("expected MulRate overflow error, got %v", err)
	}
	if _, _, err := Rescale(FromInt64(10), 31, 2, HalfEven); err == nil {
		t.Fatalf("expected Rescale scale > 30 error")
	}
	if _, _, err := Rescale(MaxAmount, 0, 5, HalfEven); !errors.Is(err, ErrOverflow) {
		t.Fatalf("expected Rescale overflow error, got %v", err)
	}
	if RoundingMode(99).String() != "RoundingMode(99)" {
		t.Fatalf("unexpected RoundingMode(99).String()")
	}

	// RoundingMode String & ParseRoundingMode
	for _, tc := range []struct {
		mode RoundingMode
		name string
	}{
		{HalfEven, "half_even"},
		{HalfUp, "half_up"},
		{Down, "down"},
		{Up, "up"},
		{Floor, "floor"},
		{Ceil, "ceil"},
	} {
		if tc.mode.String() != tc.name {
			t.Fatalf("expected %s, got %s", tc.name, tc.mode.String())
		}
		parsedMode, err := ParseRoundingMode(tc.name)
		if err != nil || parsedMode != tc.mode {
			t.Fatalf("ParseRoundingMode(%q) = %v, %v", tc.name, parsedMode, err)
		}
		// Exercise MulRate and Rescale with all rounding modes and high scale (25) to cover 256-bit division
		_, _, err = MulRate(FromParts(1, 500_000_000_000), Rate{Mantissa: FromInt64(3333333), Scale: 25}, tc.mode)
		if err != nil {
			t.Fatalf("MulRate scale 25 mode %s: %v", tc.name, err)
		}
		_, _, err = Rescale(FromInt64(-12345), 4, 2, tc.mode)
		if err != nil {
			t.Fatalf("Rescale down mode %s: %v", tc.name, err)
		}
	}
	// Rescale up and equal scale
	upScaled, rem, err := Rescale(FromInt64(123), 2, 4, HalfEven)
	if err != nil || upScaled.Cmp(FromInt64(12300)) != 0 || !rem.IsZero() {
		t.Fatalf("Rescale(123, 2, 4) = %s, %s, %v", upScaled, rem, err)
	}
	sameScaled, _, err := Rescale(FromInt64(123), 2, 2, HalfEven)
	if err != nil || sameScaled.Cmp(FromInt64(123)) != 0 {
		t.Fatalf("Rescale(123, 2, 2) = %s, %v", sameScaled, err)
	}
	if _, err := ParseRoundingMode("unknown_mode"); err == nil {
		t.Fatalf("expected ParseRoundingMode error on unknown_mode")
	}

	// Allocate edge cases (negative total, empty weights, zero weights)
	negParts, err := Allocate(FromInt64(-100), []uint64{1, 1, 1})
	if err != nil || negParts[0].Cmp(FromInt64(-34)) != 0 {
		t.Fatalf("Allocate(-100, [1,1,1]) = %v, %v", negParts, err)
	}
	if _, err := Allocate(FromInt64(100), nil); !errors.Is(err, ErrInvalidWeights) {
		t.Fatalf("expected ErrInvalidWeights on nil weights")
	}
	if _, err := Allocate(FromInt64(100), []uint64{0, 0}); !errors.Is(err, ErrInvalidWeights) {
		t.Fatalf("expected ErrInvalidWeights on all-zero weights")
	}
}

func bigDivRoundOracle(num, den *big.Int, m RoundingMode) (q, rem *big.Int) {
	if num.Sign() == 0 {
		return big.NewInt(0), big.NewInt(0)
	}
	neg := num.Sign() < 0
	absNum := new(big.Int).Abs(num)
	qAbs, rAbs := new(big.Int).QuoRem(absNum, den, new(big.Int))
	if rAbs.Sign() == 0 {
		if neg {
			qAbs.Neg(qAbs)
		}
		return qAbs, big.NewInt(0)
	}
	twoR := new(big.Int).Lsh(rAbs, 1)
	cmpHalf := twoR.Cmp(den)
	inc := false
	switch m {
	case Down:
		inc = false
	case Up:
		inc = true
	case Floor:
		inc = neg
	case Ceil:
		inc = !neg
	case HalfUp:
		inc = cmpHalf >= 0
	case HalfEven:
		inc = cmpHalf > 0 || (cmpHalf == 0 && qAbs.Bit(0) == 1)
	}
	if inc {
		qAbs.Add(qAbs, big.NewInt(1))
	}
	if neg {
		qAbs.Neg(qAbs)
	}
	qDen := new(big.Int).Mul(qAbs, den)
	rem = new(big.Int).Sub(num, qDen)
	return qAbs, rem
}

func FuzzMulRateAndRescaleOracle(f *testing.F) {
	f.Add(int64(1001), int64(29), uint8(3), uint8(0))
	f.Add(int64(-1234567), int64(9213), uint8(4), uint8(1))
	f.Add(int64(999999999), int64(12345), uint8(22), uint8(4)) // scale > 19 exercises 256-by-128 path

	f.Fuzz(func(t *testing.T, xVal, mVal int64, scale uint8, modeRaw uint8) {
		scale %= (MaxScale + 1)
		mode := RoundingMode(modeRaw % 6)
		x := FromInt64(xVal)
		r := Rate{Mantissa: FromInt64(mVal), Scale: scale}

		gotRes, gotRem, err := MulRate(x, r, mode)
		if err != nil {
			return
		}
		prodBig := new(big.Int).Mul(x.BigInt(), r.Mantissa.BigInt())
		denBig := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(scale)), nil)
		wantQ, wantRem := bigDivRoundOracle(prodBig, denBig, mode)

		if gotRes.BigInt().Cmp(wantQ) != 0 || gotRem.BigInt().Cmp(wantRem) != 0 {
			t.Fatalf("MulRate(%s, %+v, %s) = (%s, %s), want (%s, %s)",
				x, r, mode, gotRes, gotRem, wantQ, wantRem)
		}
	})
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
