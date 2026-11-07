// Package money provides 128-bit signed minor-unit arithmetic, asset scale
// formatting/parsing, exact rational rate multiplication, rescaling, and
// deterministic largest-remainder allocation without floating-point operations.
package money

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/big"
	"math/bits"
	"strconv"
	"strings"
)

var (
	// ErrOverflow indicates a 128-bit signed arithmetic overflow.
	ErrOverflow = errors.New("money: int128 overflow")
	// ErrInvalidAmount indicates an unparseable or out-of-range amount string.
	ErrInvalidAmount = errors.New("money: invalid amount")
	// ErrExtraPrecision indicates a decimal string has more fractional digits than the asset scale allows.
	ErrExtraPrecision = errors.New("money: decimal precision exceeds asset scale")
	// ErrInvalidAsset indicates a malformed asset identifier or scale out of [0, 30].
	ErrInvalidAsset = errors.New("money: invalid asset")
	// ErrInvalidWeights indicates empty or all-zero allocation weights.
	ErrInvalidWeights = errors.New("money: weights must be non-empty with positive sum")
	// ErrInvalidRate indicates an unparseable rate string.
	ErrInvalidRate = errors.New("money: invalid rate")
)

// MaxScale is the maximum supported decimal scale for an Asset or Rate.
const MaxScale = 30

// Amount is a signed 128-bit integer of minor units in two's complement representation.
// The zero value is 0.
type Amount struct {
	hi int64
	lo uint64
}

var (
	// Zero is the zero Amount.
	Zero = Amount{hi: 0, lo: 0}
	// MaxAmount is 2^127 - 1.
	MaxAmount = Amount{hi: math.MaxInt64, lo: math.MaxUint64}
	// MinAmount is -2^127.
	MinAmount = Amount{hi: math.MinInt64, lo: 0}
	// MaxPostingAmount is 2^126 - 1 (RFC section 4 & 7: posting amounts must be < 2^126).
	MaxPostingAmount = Amount{hi: (1 << 62) - 1, lo: math.MaxUint64}
)

// FromInt64 constructs an Amount from a signed 64-bit integer.
func FromInt64(v int64) Amount {
	if v < 0 {
		return Amount{hi: -1, lo: uint64(v)}
	}
	return Amount{hi: 0, lo: uint64(v)}
}

// FromUint64 constructs an Amount from an unsigned 64-bit integer.
func FromUint64(v uint64) Amount {
	return Amount{hi: 0, lo: v}
}

// FromParts constructs an Amount directly from high (signed) and low (unsigned) 64-bit words.
func FromParts(hi int64, lo uint64) Amount {
	return Amount{hi: hi, lo: lo}
}

// Parts returns the high (signed) and low (unsigned) 64-bit words of the two's complement int128.
func (a Amount) Parts() (int64, uint64) {
	return a.hi, a.lo
}

// IsZero reports whether a == 0.
func (a Amount) IsZero() bool {
	return a.hi == 0 && a.lo == 0
}

// IsNegative reports whether a < 0.
func (a Amount) IsNegative() bool {
	return a.hi < 0
}

// IsPositive reports whether a > 0.
func (a Amount) IsPositive() bool {
	return a.hi > 0 || (a.hi == 0 && a.lo > 0)
}

// Sign returns -1 if a < 0, 0 if a == 0, and +1 if a > 0.
func (a Amount) Sign() int {
	if a.hi < 0 {
		return -1
	}
	if a.hi == 0 && a.lo == 0 {
		return 0
	}
	return 1
}

// Cmp compares a and b and returns:
//
//	-1 if a <  b
//	 0 if a == b
//	+1 if a >  b
func (a Amount) Cmp(b Amount) int {
	if a.hi < b.hi {
		return -1
	}
	if a.hi > b.hi {
		return 1
	}
	if a.lo < b.lo {
		return -1
	}
	if a.lo > b.lo {
		return 1
	}
	return 0
}

// Add returns a + b, or ErrOverflow if the result exceeds int128 range.
func (a Amount) Add(b Amount) (Amount, error) {
	lo, carry := bits.Add64(a.lo, b.lo, 0)
	uHi, _ := bits.Add64(uint64(a.hi), uint64(b.hi), carry)
	hi := int64(uHi)
	// Signed overflow occurs if and only if a and b have the same sign,
	// and the result has the opposite sign.
	if (a.hi^b.hi) >= 0 && (a.hi^hi) < 0 {
		return Zero, ErrOverflow
	}
	return Amount{hi: hi, lo: lo}, nil
}

// Sub returns a - b, or ErrOverflow if the result exceeds int128 range.
func (a Amount) Sub(b Amount) (Amount, error) {
	lo, borrow := bits.Sub64(a.lo, b.lo, 0)
	uHi, _ := bits.Sub64(uint64(a.hi), uint64(b.hi), borrow)
	hi := int64(uHi)
	// Signed overflow occurs if a and b have different signs and the result sign != a's sign.
	if (a.hi^b.hi) < 0 && (a.hi^hi) < 0 {
		return Zero, ErrOverflow
	}
	return Amount{hi: hi, lo: lo}, nil
}

// Neg returns -a, or ErrOverflow if a == MinAmount (-2^127).
func (a Amount) Neg() (Amount, error) {
	if a == MinAmount {
		return Zero, ErrOverflow
	}
	return Zero.Sub(a)
}

// Abs returns |a|, or ErrOverflow if a == MinAmount.
func (a Amount) Abs() (Amount, error) {
	if a.hi < 0 {
		return a.Neg()
	}
	return a, nil
}

// Int64 returns the int64 value of a if it fits, or (0, false) otherwise.
func (a Amount) Int64() (int64, bool) {
	if a.hi == 0 && a.lo <= math.MaxInt64 {
		return int64(a.lo), true
	}
	if a.hi == -1 && a.lo >= (1<<63) {
		return int64(a.lo), true
	}
	return 0, false
}

// BigInt converts Amount to a *big.Int.
func (a Amount) BigInt() *big.Int {
	if a.IsZero() {
		return big.NewInt(0)
	}
	neg := a.hi < 0
	uHi := uint64(a.hi)
	uLo := a.lo
	if neg {
		var borrow uint64
		uLo, borrow = bits.Sub64(0, uLo, 0)
		uHi, _ = bits.Sub64(0, uHi, borrow)
	}
	z := new(big.Int).SetUint64(uHi)
	z.Lsh(z, 64)
	z.Or(z, new(big.Int).SetUint64(uLo))
	if neg {
		z.Neg(z)
	}
	return z
}

// FromBigInt converts a *big.Int to Amount, returning ErrOverflow if out of int128 bounds.
func FromBigInt(b *big.Int) (Amount, error) {
	if b == nil || b.Sign() == 0 {
		return Zero, nil
	}
	neg := b.Sign() < 0
	abs := new(big.Int).Abs(b)
	if abs.BitLen() > 128 {
		return Zero, ErrOverflow
	}
	// Extract low 64 and high 64 bits of magnitude
	var uLo, uHi uint64
	words := abs.Bits()
	if bits.UintSize == 64 {
		if len(words) >= 1 {
			uLo = uint64(words[0])
		}
		if len(words) >= 2 {
			uHi = uint64(words[1])
		}
		if len(words) > 2 {
			return Zero, ErrOverflow
		}
	} else {
		// 32-bit fallback
		mask64 := new(big.Int).SetUint64(math.MaxUint64)
		uLo = new(big.Int).And(abs, mask64).Uint64()
		uHi = new(big.Int).Rsh(abs, 64).Uint64()
	}

	if !neg {
		if uHi > math.MaxInt64 {
			return Zero, ErrOverflow
		}
		return Amount{hi: int64(uHi), lo: uLo}, nil
	}
	// Negative: magnitude up to 2^127 is valid (-2^127 has uHi == 1<<63 && uLo == 0)
	if uHi > (1<<63) || (uHi == (1<<63) && uLo > 0) {
		return Zero, ErrOverflow
	}
	var borrow uint64
	lo, borrow := bits.Sub64(0, uLo, 0)
	hiU, _ := bits.Sub64(0, uHi, borrow)
	return Amount{hi: int64(hiU), lo: lo}, nil
}

// String returns the canonical base-10 string of minor units (e.g. "1001" or "-45").
func (a Amount) String() string {
	if a.hi == 0 {
		return strconv.FormatUint(a.lo, 10)
	}
	if v, ok := a.Int64(); ok {
		return strconv.FormatInt(v, 10)
	}
	return a.BigInt().String()
}

// ParseMinor parses a canonical base-10 integer string of minor units (e.g., "1001").
func ParseMinor(s string) (Amount, error) {
	if s == "" {
		return Zero, ErrInvalidAmount
	}
	start := 0
	if s[0] == '-' || s[0] == '+' {
		start = 1
	}
	if start == len(s) {
		return Zero, ErrInvalidAmount
	}
	for i := start; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return Zero, ErrInvalidAmount
		}
	}
	if v, err := strconv.ParseInt(s, 10, 64); err == nil {
		return FromInt64(v), nil
	}
	z, ok := new(big.Int).SetString(s, 10)
	if !ok {
		return Zero, ErrInvalidAmount
	}
	return FromBigInt(z)
}

// MarshalJSON encodes Amount as a JSON string of minor units.
func (a Amount) MarshalJSON() ([]byte, error) {
	return json.Marshal(a.String())
}

// UnmarshalJSON decodes Amount from a JSON string of minor units.
func (a *Amount) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}
	v, err := ParseMinor(s)
	if err != nil {
		return err
	}
	*a = v
	return nil
}

// AssetID identifies an asset and its scale, formatted as "<CODE>/<SCALE>" (e.g., "USD/2").
type AssetID string

// Asset represents a currency or unit and its fixed decimal scale.
type Asset struct {
	Code  string // e.g., "USD", "JPY", "ETH", "AAPL"
	Scale uint8  // 0..30
}

// ID returns the canonical AssetID string ("<Code>/<Scale>").
func (a Asset) ID() AssetID {
	return AssetID(fmt.Sprintf("%s/%d", a.Code, a.Scale))
}

// Validate checks that the Asset has a non-empty code without '/' and a scale in [0, MaxScale].
func (a Asset) Validate() error {
	if a.Code == "" || strings.Contains(a.Code, "/") || strings.TrimSpace(a.Code) != a.Code {
		return fmt.Errorf("%w: invalid code %q", ErrInvalidAsset, a.Code)
	}
	if a.Scale > MaxScale {
		return fmt.Errorf("%w: scale %d exceeds max %d", ErrInvalidAsset, a.Scale, MaxScale)
	}
	return nil
}

// ParseAsset parses an AssetID such as "USD/2" into an Asset.
func ParseAsset(id AssetID) (Asset, error) {
	parts := strings.Split(string(id), "/")
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return Asset{}, fmt.Errorf("%w: %q", ErrInvalidAsset, id)
	}
	scale64, err := strconv.ParseUint(parts[1], 10, 8)
	if err != nil {
		return Asset{}, fmt.Errorf("%w: %q", ErrInvalidAsset, id)
	}
	a := Asset{
		Code:  parts[0],
		Scale: uint8(scale64),
	}
	if err := a.Validate(); err != nil {
		return Asset{}, err
	}
	return a, nil
}

// Parse converts a human-readable decimal string (e.g., "10.01") into minor units
// at the asset's scale. It REJECTS extra fractional digits ("10.015" for USD/2)
// instead of rounding silently.
func Parse(s string, a Asset) (Amount, error) {
	if err := a.Validate(); err != nil {
		return Zero, err
	}
	if s == "" {
		return Zero, ErrInvalidAmount
	}
	neg := false
	raw := s
	if raw[0] == '-' {
		neg = true
		raw = raw[1:]
	} else if raw[0] == '+' {
		raw = raw[1:]
	}
	if raw == "" {
		return Zero, ErrInvalidAmount
	}

	var wholePart, fracPart string
	dotIdx := strings.IndexByte(raw, '.')
	if dotIdx == -1 {
		wholePart = raw
	} else {
		if strings.IndexByte(raw[dotIdx+1:], '.') != -1 {
			return Zero, ErrInvalidAmount
		}
		wholePart = raw[:dotIdx]
		fracPart = raw[dotIdx+1:]
		if wholePart == "" || fracPart == "" {
			return Zero, ErrInvalidAmount
		}
	}

	for i := 0; i < len(wholePart); i++ {
		if wholePart[i] < '0' || wholePart[i] > '9' {
			return Zero, ErrInvalidAmount
		}
	}
	for i := 0; i < len(fracPart); i++ {
		if fracPart[i] < '0' || fracPart[i] > '9' {
			return Zero, ErrInvalidAmount
		}
	}

	if len(fracPart) > int(a.Scale) {
		return Zero, fmt.Errorf("%w: %q has %d fractional digits, asset %s allows %d",
			ErrExtraPrecision, s, len(fracPart), a.ID(), a.Scale)
	}

	var b strings.Builder
	if neg {
		b.WriteByte('-')
	}
	b.WriteString(wholePart)
	b.WriteString(fracPart)
	for i := len(fracPart); i < int(a.Scale); i++ {
		b.WriteByte('0')
	}

	amt, err := ParseMinor(b.String())
	if err != nil {
		return Zero, err
	}
	// "-0" normalises to 0
	return amt, nil
}

// Format renders an Amount as a human-readable decimal string at the asset's scale (e.g. "10.01").
func Format(x Amount, a Asset) string {
	neg := x.IsNegative()
	absStr := x.String()
	if neg {
		absStr = absStr[1:]
	}
	scale := int(a.Scale)
	if scale == 0 {
		if neg {
			return "-" + absStr
		}
		return absStr
	}

	var b strings.Builder
	if neg {
		b.WriteByte('-')
	}
	if len(absStr) <= scale {
		b.WriteString("0.")
		for i := 0; i < scale-len(absStr); i++ {
			b.WriteByte('0')
		}
		b.WriteString(absStr)
	} else {
		split := len(absStr) - scale
		b.WriteString(absStr[:split])
		b.WriteByte('.')
		b.WriteString(absStr[split:])
	}
	return b.String()
}

// Rate represents an exact decimal rational: Mantissa * 10^-Scale.
// For example, 2.9% is Rate{Mantissa: FromInt64(29), Scale: 3}.
type Rate struct {
	Mantissa Amount
	Scale    uint8
}

// ParseRate parses a decimal string such as "0.029" or "187.1234" into a Rate.
func ParseRate(s string) (Rate, error) {
	if s == "" {
		return Rate{}, ErrInvalidRate
	}
	neg := false
	raw := s
	if raw[0] == '-' {
		neg = true
		raw = raw[1:]
	} else if raw[0] == '+' {
		raw = raw[1:]
	}
	if raw == "" {
		return Rate{}, ErrInvalidRate
	}
	var wholePart, fracPart string
	dotIdx := strings.IndexByte(raw, '.')
	if dotIdx == -1 {
		wholePart = raw
	} else {
		if strings.IndexByte(raw[dotIdx+1:], '.') != -1 {
			return Rate{}, ErrInvalidRate
		}
		wholePart = raw[:dotIdx]
		fracPart = raw[dotIdx+1:]
		if wholePart == "" || fracPart == "" {
			return Rate{}, ErrInvalidRate
		}
	}
	if len(fracPart) > MaxScale {
		return Rate{}, fmt.Errorf("%w: scale %d exceeds max %d", ErrInvalidRate, len(fracPart), MaxScale)
	}
	combined := wholePart + fracPart
	if neg {
		combined = "-" + combined
	}
	m, err := ParseMinor(combined)
	if err != nil {
		return Rate{}, ErrInvalidRate
	}
	return Rate{
		Mantissa: m,
		Scale:    uint8(len(fracPart)),
	}, nil
}

// String formats Rate as a decimal string.
func (r Rate) String() string {
	return Format(r.Mantissa, Asset{Code: "RATE", Scale: r.Scale})
}

// RoundingMode controls how non-integral quotients are rounded to integer minor units.
type RoundingMode uint8

const (
	// HalfEven rounds to nearest, ties to even (banker's rounding; default).
	HalfEven RoundingMode = iota
	// HalfUp rounds to nearest, ties away from zero.
	HalfUp
	// Down rounds toward zero (truncation).
	Down
	// Up rounds away from zero.
	Up
	// Floor rounds toward negative infinity.
	Floor
	// Ceil rounds toward positive infinity.
	Ceil
)

// String returns the name of the RoundingMode.
func (m RoundingMode) String() string {
	switch m {
	case HalfEven:
		return "half_even"
	case HalfUp:
		return "half_up"
	case Down:
		return "down"
	case Up:
		return "up"
	case Floor:
		return "floor"
	case Ceil:
		return "ceil"
	default:
		return fmt.Sprintf("RoundingMode(%d)", m)
	}
}

// ParseRoundingMode parses a rounding mode string.
func ParseRoundingMode(s string) (RoundingMode, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "", "half_even", "halfeven":
		return HalfEven, nil
	case "half_up", "halfup":
		return HalfUp, nil
	case "down", "truncate":
		return Down, nil
	case "up":
		return Up, nil
	case "floor":
		return Floor, nil
	case "ceil", "ceiling":
		return Ceil, nil
	default:
		return HalfEven, fmt.Errorf("money: unknown rounding mode %q", s)
	}
}

func pow10Big(exp uint8) *big.Int {
	return new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(exp)), nil)
}

// divRound divides num by den (den > 0) using RoundingMode m, returning the rounded
// quotient q and the exact remainder r = num - q*den.
func divRound(num, den *big.Int, m RoundingMode) (q, rem *big.Int) {
	if num.Sign() == 0 {
		return big.NewInt(0), big.NewInt(0)
	}
	neg := num.Sign() < 0
	absNum := new(big.Int).Abs(num)

	// Truncated quotient and remainder on magnitudes: absNum = qAbs * den + rAbs
	qAbs, rAbs := new(big.Int).QuoRem(absNum, den, new(big.Int))
	if rAbs.Sign() == 0 {
		if neg {
			qAbs.Neg(qAbs)
		}
		return qAbs, big.NewInt(0)
	}

	// Determine whether to increment magnitude qAbs by 1
	inc := false
	// Compare 2 * rAbs with den
	twoR := new(big.Int).Lsh(rAbs, 1)
	cmpHalf := twoR.Cmp(den)

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
		if cmpHalf > 0 {
			inc = true
		} else if cmpHalf == 0 {
			// Tie: round to even
			if qAbs.Bit(0) == 1 {
				inc = true
			}
		}
	}

	if inc {
		qAbs.Add(qAbs, big.NewInt(1))
	}
	if neg {
		qAbs.Neg(qAbs)
	}
	// Exact remainder: num - q * den
	qDen := new(big.Int).Mul(qAbs, den)
	rem = new(big.Int).Sub(num, qDen)
	return qAbs, rem
}

// MulRate returns x * r rounded to x's scale using a 256-bit intermediate.
// It also returns the remainder that rounding discarded, in units of 10^-r.Scale
// (so x * r.Mantissa == result * 10^r.Scale + remainder).
func MulRate(x Amount, r Rate, m RoundingMode) (result Amount, remainder Amount, err error) {
	if r.Scale > MaxScale {
		return Zero, Zero, fmt.Errorf("%w: scale %d exceeds max %d", ErrInvalidRate, r.Scale, MaxScale)
	}
	prod := new(big.Int).Mul(x.BigInt(), r.Mantissa.BigInt())
	den := pow10Big(r.Scale)

	qBig, remBig := divRound(prod, den, m)
	resAmt, err := FromBigInt(qBig)
	if err != nil {
		return Zero, Zero, err
	}
	remAmt, err := FromBigInt(remBig)
	if err != nil {
		return Zero, Zero, err
	}
	return resAmt, remAmt, nil
}

// Rescale moves an amount between scales of the same currency (e.g. USD/6 -> USD/2).
// When scaling down (from > to), remainder is returned in the source (`from`) scale
// so that x == result * 10^(from-to) + remainder.
func Rescale(x Amount, from, to uint8, m RoundingMode) (result, remainder Amount, err error) {
	if from > MaxScale || to > MaxScale {
		return Zero, Zero, ErrInvalidAsset
	}
	if from == to {
		return x, Zero, nil
	}
	if to > from {
		factor := pow10Big(to - from)
		scaled := new(big.Int).Mul(x.BigInt(), factor)
		resAmt, err := FromBigInt(scaled)
		if err != nil {
			return Zero, Zero, err
		}
		return resAmt, Zero, nil
	}
	// Scaling down: from > to
	den := pow10Big(from - to)
	qBig, remBig := divRound(x.BigInt(), den, m)
	resAmt, err := FromBigInt(qBig)
	if err != nil {
		return Zero, Zero, err
	}
	remAmt, err := FromBigInt(remBig)
	if err != nil {
		return Zero, Zero, err
	}
	return resAmt, remAmt, nil
}

// Allocate splits total by weights using the largest-remainder method.
// Parts always sum to total; ties go to the lowest index so results are deterministic.
func Allocate(total Amount, weights []uint64) ([]Amount, error) {
	if len(weights) == 0 {
		return nil, ErrInvalidWeights
	}
	weightSum := new(big.Int)
	for _, w := range weights {
		weightSum.Add(weightSum, new(big.Int).SetUint64(w))
	}
	if weightSum.Sign() <= 0 {
		return nil, ErrInvalidWeights
	}

	neg := total.IsNegative()
	absTotal := new(big.Int).Abs(total.BigInt())

	type part struct {
		idx       int
		quotient  *big.Int
		remainder *big.Int
	}

	parts := make([]part, len(weights))
	allocatedSum := new(big.Int)

	for i, w := range weights {
		num := new(big.Int).Mul(absTotal, new(big.Int).SetUint64(w))
		q, rem := new(big.Int).QuoRem(num, weightSum, new(big.Int))
		parts[i] = part{
			idx:       i,
			quotient:  q,
			remainder: rem,
		}
		allocatedSum.Add(allocatedSum, q)
	}

	leftover := new(big.Int).Sub(absTotal, allocatedSum).Int64()
	// Sort indices by largest remainder, breaking ties by lowest index
	order := make([]int, len(weights))
	for i := range order {
		order[i] = i
	}
	// Deterministic insertion sort (typically small N) or stable sort
	for i := 1; i < len(order); i++ {
		cur := order[i]
		j := i - 1
		for j >= 0 {
			prev := order[j]
			cmp := parts[cur].remainder.Cmp(parts[prev].remainder)
			if cmp > 0 || (cmp == 0 && cur < prev) {
				order[j+1] = order[j]
				j--
			} else {
				break
			}
		}
		order[j+1] = cur
	}

	one := big.NewInt(1)
	for i := int64(0); i < leftover; i++ {
		targetIdx := order[i]
		parts[targetIdx].quotient.Add(parts[targetIdx].quotient, one)
	}

	out := make([]Amount, len(weights))
	for i := range parts {
		val := parts[i].quotient
		if neg && val.Sign() > 0 {
			val = new(big.Int).Neg(val)
		}
		amt, err := FromBigInt(val)
		if err != nil {
			return nil, err
		}
		out[i] = amt
	}
	return out, nil
}
