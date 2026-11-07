package core

import (
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"math/big"
	"sync"
	"time"
)

// Clock abstracts time so modules and simulation tests never call time.Now() directly.
type Clock interface {
	Now() time.Time
}

// RealClock returns wall-clock UTC time.
type RealClock struct{}

func (RealClock) Now() time.Time {
	return time.Now().UTC()
}

// ManualClock is a thread-safe controllable clock for tests and deterministic simulation.
type ManualClock struct {
	mu  sync.RWMutex
	now time.Time
}

// NewManualClock creates a ManualClock starting at t.
func NewManualClock(t time.Time) *ManualClock {
	return &ManualClock{now: t.UTC()}
}

func (c *ManualClock) Now() time.Time {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.now
}

// Advance adds d to the current clock time.
func (c *ManualClock) Advance(d time.Duration) time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
	return c.now
}

// Set sets the current clock time to t.
func (c *ManualClock) Set(t time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = t.UTC()
}

// IDGenerator produces monotonically time-ordered UUIDv7 strings.
type IDGenerator interface {
	NewID(now time.Time) string
}

// UUIDv7Generator generates RFC 9562 UUIDv7 strings with a monotonic counter per millisecond.
type UUIDv7Generator struct {
	mu     sync.Mutex
	lastMS int64
	seq    uint16
}

// NewUUIDv7Generator returns a production UUIDv7Generator.
func NewUUIDv7Generator() *UUIDv7Generator {
	return &UUIDv7Generator{}
}

// NewID generates a canonical 36-char UUIDv7 string for timestamp now.
func (g *UUIDv7Generator) NewID(now time.Time) string {
	g.mu.Lock()
	defer g.mu.Unlock()

	ms := now.UnixMilli()
	if ms < 0 {
		ms = 0
	}
	var randBytes [10]byte
	_, _ = rand.Read(randBytes[:])

	if ms == g.lastMS {
		g.seq++
	} else {
		if ms > g.lastMS {
			g.lastMS = ms
		} else {
			ms = g.lastMS
		}
		g.seq = binary.BigEndian.Uint16(randBytes[:2]) & 0x0fff
	}

	return formatUUIDv7(uint64(ms), g.seq, randBytes[2:])
}

// DeterministicIDGenerator produces reproducible UUIDv7 strings from a seed for simulation.
type DeterministicIDGenerator struct {
	mu      sync.Mutex
	counter uint64
	seed    uint64
}

// NewDeterministicIDGenerator returns a seeded deterministic UUIDv7 generator.
func NewDeterministicIDGenerator(seed uint64) *DeterministicIDGenerator {
	return &DeterministicIDGenerator{seed: seed}
}

func (g *DeterministicIDGenerator) NewID(now time.Time) string {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.counter++

	ms := now.UnixMilli()
	if ms < 0 {
		ms = 0
	}
	seq := uint16(g.counter & 0x0fff)
	var tail [8]byte
	// Mix seed and counter deterministically
	v := g.seed ^ (g.counter * 0x9e3779b97f4a7c15)
	binary.BigEndian.PutUint64(tail[:], v)
	return formatUUIDv7(uint64(ms), seq, tail[:])
}

func formatUUIDv7(ms uint64, seq12 uint16, tail8 []byte) string {
	var b [16]byte
	// 48-bit big-endian unix_ts_ms
	b[0] = byte(ms >> 40)
	b[1] = byte(ms >> 32)
	b[2] = byte(ms >> 24)
	b[3] = byte(ms >> 16)
	b[4] = byte(ms >> 8)
	b[5] = byte(ms)

	// ver (4 bits = 0111) + rand_a (12 bits = seq12)
	b[6] = 0x70 | byte((seq12>>8)&0x0f)
	b[7] = byte(seq12 & 0xff)

	// var (2 bits = 10) + rand_b (62 bits)
	copy(b[8:], tail8[:8])
	b[8] = (b[8] & 0x3f) | 0x80

	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		binary.BigEndian.Uint32(b[0:4]),
		binary.BigEndian.Uint16(b[4:6]),
		binary.BigEndian.Uint16(b[6:8]),
		binary.BigEndian.Uint16(b[8:10]),
		b[10:16],
	)
}

// Unused import guard for big if needed
var _ = (*big.Int)(nil)
