package money

import (
	"math"
	"math/bits"
)

// u128 is an unsigned 128-bit integer on the stack.
type u128 struct {
	hi uint64
	lo uint64
}

// u256 is an unsigned 256-bit integer on the stack (w0 is least significant, w3 is most significant).
type u256 struct {
	w0 uint64
	w1 uint64
	w2 uint64
	w3 uint64
}

// pow10Table holds 10^0 .. 10^30 as precomputed unsigned 128-bit constants.
var pow10Table = [MaxScale + 1]u128{
	0:  {0, 1},
	1:  {0, 10},
	2:  {0, 100},
	3:  {0, 1_000},
	4:  {0, 10_000},
	5:  {0, 100_000},
	6:  {0, 1_000_000},
	7:  {0, 10_000_000},
	8:  {0, 100_000_000},
	9:  {0, 1_000_000_000},
	10: {0, 10_000_000_000},
	11: {0, 100_000_000_000},
	12: {0, 1_000_000_000_000},
	13: {0, 10_000_000_000_000},
	14: {0, 100_000_000_000_000},
	15: {0, 1_000_000_000_000_000},
	16: {0, 10_000_000_000_000_000},
	17: {0, 100_000_000_000_000_000},
	18: {0, 1_000_000_000_000_000_000},
	19: {0, 10_000_000_000_000_000_000},
	20: {5, 7766279631452241920},
	21: {54, 3875820019684212736},
	22: {542, 1864712049423024128},
	23: {5421, 200376420520689664},
	24: {54210, 2003764205206896640},
	25: {542101, 1590897978359414784},
	26: {5421010, 15908979783594147840},
	27: {54210108, 11515845246265065472},
	28: {542101086, 4477988020393345024},
	29: {5421010862, 7886392056514347008},
	30: {54210108624, 5076944270305263616},
}

func absU128(a Amount) (u128, bool) {
	if a.hi >= 0 {
		return u128{hi: uint64(a.hi), lo: a.lo}, false
	}
	lo, borrow := bits.Sub64(0, a.lo, 0)
	hi, _ := bits.Sub64(0, uint64(a.hi), borrow)
	return u128{hi: hi, lo: lo}, true
}

func fromSignedU128(u u128, neg bool) (Amount, error) {
	if u.hi == 0 && u.lo == 0 {
		return Zero, nil
	}
	if !neg {
		if u.hi > math.MaxInt64 {
			return Zero, ErrOverflow
		}
		return Amount{hi: int64(u.hi), lo: u.lo}, nil
	}
	if u.hi > (1<<63) || (u.hi == (1<<63) && u.lo > 0) {
		return Zero, ErrOverflow
	}
	lo, borrow := bits.Sub64(0, u.lo, 0)
	hi, _ := bits.Sub64(0, u.hi, borrow)
	return Amount{hi: int64(hi), lo: lo}, nil
}

func fromSignedU256(u u256, neg bool) (Amount, error) {
	if u.w2 != 0 || u.w3 != 0 {
		return Zero, ErrOverflow
	}
	return fromSignedU128(u128{hi: u.w1, lo: u.w0}, neg)
}

func (u u128) cmp(v u128) int {
	if u.hi < v.hi {
		return -1
	}
	if u.hi > v.hi {
		return 1
	}
	if u.lo < v.lo {
		return -1
	}
	if u.lo > v.lo {
		return 1
	}
	return 0
}

func (u u128) isZero() bool {
	return u.hi == 0 && u.lo == 0
}

// mul128 computes the exact 256-bit product of two 128-bit unsigned integers.
func mul128(a, b u128) u256 {
	// a.lo * b.lo
	p0Hi, p0Lo := bits.Mul64(a.lo, b.lo)
	// a.lo * b.hi
	p1Hi, p1Lo := bits.Mul64(a.lo, b.hi)
	// a.hi * b.lo
	p2Hi, p2Lo := bits.Mul64(a.hi, b.lo)
	// a.hi * b.hi
	p3Hi, p3Lo := bits.Mul64(a.hi, b.hi)

	var c1, c2, c3 uint64
	w0 := p0Lo
	w1, c1 := bits.Add64(p0Hi, p1Lo, 0)
	w1, c2 = bits.Add64(w1, p2Lo, 0)

	w2, c3 := bits.Add64(p1Hi, p2Hi, c1+c2)
	var c4 uint64
	w2, c4 = bits.Add64(w2, p3Lo, 0)

	w3 := p3Hi + c3 + c4
	return u256{w0: w0, w1: w1, w2: w2, w3: w3}
}

func (u u256) isZero() bool {
	return u.w0 == 0 && u.w1 == 0 && u.w2 == 0 && u.w3 == 0
}

func (u u256) addUint64(v uint64) u256 {
	w0, c := bits.Add64(u.w0, v, 0)
	w1, c := bits.Add64(u.w1, 0, c)
	w2, c := bits.Add64(u.w2, 0, c)
	w3 := u.w3 + c
	return u256{w0: w0, w1: w1, w2: w2, w3: w3}
}

func (u u256) sub(v u256) u256 {
	w0, b := bits.Sub64(u.w0, v.w0, 0)
	w1, b := bits.Sub64(u.w1, v.w1, b)
	w2, b := bits.Sub64(u.w2, v.w2, b)
	w3, _ := bits.Sub64(u.w3, v.w3, b)
	return u256{w0: w0, w1: w1, w2: w2, w3: w3}
}

// divRem256 divides num (256-bit) by den (128-bit, > 0), returning quotient q and remainder rem.
func divRem256(num u256, den u128) (q u256, rem u128) {
	if num.isZero() {
		return u256{}, u128{}
	}
	// Fast path when den fits in 64 bits (covers 10^0 .. 10^19, i.e. scales 0..19)
	if den.hi == 0 {
		d := den.lo
		q3, r3 := bits.Div64(0, num.w3, d)
		q2, r2 := bits.Div64(r3, num.w2, d)
		q1, r1 := bits.Div64(r2, num.w1, d)
		q0, r0 := bits.Div64(r1, num.w0, d)
		return u256{w0: q0, w1: q1, w2: q2, w3: q3}, u128{hi: 0, lo: r0}
	}

	// General 256-by-128 bitwise restoring division on stack (zero allocations)
	var r u256 // running remainder
	den256 := u256{w0: den.lo, w1: den.hi}
	for i := 255; i >= 0; i-- {
		// Left shift r by 1 and bring in bit i of num
		bit := num.bit(uint(i))
		r = r.lsh1(bit)
		if r.cmp(den256) >= 0 {
			r = r.sub(den256)
			q = q.setBit(uint(i))
		}
	}
	return q, u128{hi: r.w1, lo: r.w0}
}

func (u u256) bit(i uint) uint64 {
	word := i / 64
	shift := i % 64
	switch word {
	case 0:
		return (u.w0 >> shift) & 1
	case 1:
		return (u.w1 >> shift) & 1
	case 2:
		return (u.w2 >> shift) & 1
	default:
		return (u.w3 >> shift) & 1
	}
}

func (u u256) setBit(i uint) u256 {
	word := i / 64
	mask := uint64(1) << (i % 64)
	switch word {
	case 0:
		u.w0 |= mask
	case 1:
		u.w1 |= mask
	case 2:
		u.w2 |= mask
	default:
		u.w3 |= mask
	}
	return u
}

func (u u256) lsh1(lowBit uint64) u256 {
	return u256{
		w3: (u.w3 << 1) | (u.w2 >> 63),
		w2: (u.w2 << 1) | (u.w1 >> 63),
		w1: (u.w1 << 1) | (u.w0 >> 63),
		w0: (u.w0 << 1) | (lowBit & 1),
	}
}

func (u u256) cmp(v u256) int {
	if u.w3 != v.w3 {
		if u.w3 < v.w3 {
			return -1
		}
		return 1
	}
	if u.w2 != v.w2 {
		if u.w2 < v.w2 {
			return -1
		}
		return 1
	}
	if u.w1 != v.w1 {
		if u.w1 < v.w1 {
			return -1
		}
		return 1
	}
	if u.w0 != v.w0 {
		if u.w0 < v.w0 {
			return -1
		}
		return 1
	}
	return 0
}

// divRound256 divides signed 256-bit (absNum, neg) by positive 128-bit den using RoundingMode m,
// returning rounded Amount and exact remainder Amount (num - q * den) with zero heap allocations.
func divRound256(absNum u256, neg bool, den u128, m RoundingMode) (Amount, Amount, error) {
	if absNum.isZero() {
		return Zero, Zero, nil
	}
	qAbs, rAbs := divRem256(absNum, den)
	if rAbs.isZero() {
		qAmt, err := fromSignedU256(qAbs, neg)
		return qAmt, Zero, err
	}

	// Compare 2 * rAbs with den (note: rAbs < den, so 2*rAbs fits in u256)
	twoR := u256{w0: rAbs.lo, w1: rAbs.hi}.lsh1(0)
	den256 := u256{w0: den.lo, w1: den.hi}
	cmpHalf := twoR.cmp(den256)

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
		if cmpHalf > 0 {
			inc = true
		} else if cmpHalf == 0 {
			if (qAbs.w0 & 1) == 1 {
				inc = true
			}
		}
	}

	var remAmt Amount
	var err error
	if inc {
		qAbs = qAbs.addUint64(1)
		// Magnitude of remainder is den - rAbs, with opposite sign of num
		diffLo, borrow := bits.Sub64(den.lo, rAbs.lo, 0)
		diffHi, _ := bits.Sub64(den.hi, rAbs.hi, borrow)
		remAmt, err = fromSignedU128(u128{hi: diffHi, lo: diffLo}, !neg)
		if err != nil {
			return Zero, Zero, err
		}
	} else {
		// Remainder is rAbs with same sign as num
		remAmt, err = fromSignedU128(rAbs, neg)
		if err != nil {
			return Zero, Zero, err
		}
	}

	qAmt, err := fromSignedU256(qAbs, neg)
	if err != nil {
		return Zero, Zero, err
	}
	return qAmt, remAmt, nil
}
