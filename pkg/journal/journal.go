// Package journal implements the tamper-evident SHA-256 hash chain, RFC 8785
// JSON Canonicalization Scheme (JCS), periodic checkpoints, and chain verification.
package journal

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/mtfuller/go-finjournal/pkg/core"
	"github.com/mtfuller/go-finjournal/pkg/events"
	"github.com/mtfuller/go-finjournal/pkg/storage"
)

// GenesisHash computes the deterministic genesis chain hash for a ledger.
func GenesisHash(l core.LedgerID) []byte {
	sum := sha256.Sum256([]byte("go-finjournal:genesis:" + string(l)))
	return sum[:]
}

// CanonicalTransactionJSON serializes the immutable fields of tx into deterministic
// RFC 8785 Canonical JSON (JCS): keys sorted lexicographically in UTF-16/UTF-8 order,
// no whitespace, canonical string escaping, and UTC RFC3339Nano timestamps.
func CanonicalTransactionJSON(tx *core.Transaction) ([]byte, error) {
	var b bytes.Buffer
	b.WriteByte('{')

	// Keys in alphabetical order:
	// "committed_at", "effective_at", "id", "idempotency_key", "kind", "ledger",
	// "metadata", "pending_id", "postings", "reverses"
	writeKVString(&b, "committed_at", tx.CommittedAt.UTC().Format(time.RFC3339Nano), true)
	writeKVString(&b, "effective_at", tx.EffectiveAt.UTC().Format(time.RFC3339Nano), false)
	writeKVString(&b, "id", string(tx.ID), false)
	writeKVString(&b, "idempotency_key", tx.IdempotencyKey, false)
	writeKVString(&b, "kind", tx.Kind, false)
	writeKVString(&b, "ledger", string(tx.Ledger), false)

	// metadata (sorted keys)
	b.WriteString(`,"metadata":{`)
	if len(tx.Metadata) > 0 {
		keys := make([]string, 0, len(tx.Metadata))
		for k := range tx.Metadata {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for i, k := range keys {
			writeKVString(&b, k, tx.Metadata[k], i == 0)
		}
	}
	b.WriteByte('}')

	// pending_id
	if tx.PendingID != nil {
		writeKVString(&b, "pending_id", string(*tx.PendingID), false)
	} else {
		b.WriteString(`,"pending_id":null`)
	}

	// postings array
	b.WriteString(`,"postings":[`)
	for i, p := range tx.Postings {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteByte('{')
		writeKVString(&b, "amount", p.Amount.String(), true)
		writeKVString(&b, "asset", string(p.Asset), false)
		writeKVString(&b, "credit", string(p.Credit), false)
		writeKVString(&b, "debit", string(p.Debit), false)
		writeKVString(&b, "memo", p.Memo, false)
		writeKVString(&b, "source", p.Source, false)
		b.WriteByte('}')
	}
	b.WriteByte(']')

	// reverses
	if tx.Reverses != nil {
		writeKVString(&b, "reverses", string(*tx.Reverses), false)
	} else {
		b.WriteString(`,"reverses":null`)
	}

	b.WriteByte('}')
	return b.Bytes(), nil
}

func writeKVString(b *bytes.Buffer, key, val string, first bool) {
	if !first {
		b.WriteByte(',')
	}
	kb, _ := json.Marshal(key)
	vb, _ := json.Marshal(val)
	b.Write(kb)
	b.WriteByte(':')
	b.Write(vb)
}

// TxHash computes SHA-256(JCS(tx)).
func TxHash(tx *core.Transaction) []byte {
	jcs, _ := CanonicalTransactionJSON(tx)
	sum := sha256.Sum256(jcs)
	return sum[:]
}

// ChainHash computes chain_hash(n) = SHA-256(chain_hash(n-1) || seq(n) || tx_hash(n)).
func ChainHash(prevChainHash []byte, seq uint64, txHash []byte) []byte {
	h := sha256.New()
	h.Write(prevChainHash)
	var seqBuf [8]byte
	binary.BigEndian.PutUint64(seqBuf[:], seq)
	h.Write(seqBuf[:])
	h.Write(txHash)
	return h.Sum(nil)
}

// SealerConfig configures the hash-chain sealer for a ledger.
type SealerConfig struct {
	CheckpointEvery uint64        // default 10,000 transactions
	CheckpointMaxAge time.Duration // default 60s
	BatchLimit      int           // default 500
	Clock           core.Clock
	IDGen           core.IDGenerator
}

// Sealer extends the tamper-evident hash chain for a ledger.
type Sealer struct {
	store          storage.Store
	cfg            SealerConfig
	lastCheckpoint map[core.LedgerID]time.Time
}

// NewSealer constructs a Sealer for store.
func NewSealer(store storage.Store, cfg SealerConfig) *Sealer {
	if cfg.CheckpointEvery == 0 {
		cfg.CheckpointEvery = 10_000
	}
	if cfg.CheckpointMaxAge <= 0 {
		cfg.CheckpointMaxAge = 60 * time.Second
	}
	if cfg.BatchLimit <= 0 {
		cfg.BatchLimit = 500
	}
	if cfg.Clock == nil {
		cfg.Clock = core.RealClock{}
	}
	if cfg.IDGen == nil {
		cfg.IDGen = core.NewUUIDv7Generator()
	}
	return &Sealer{
		store:          store,
		cfg:            cfg,
		lastCheckpoint: make(map[core.LedgerID]time.Time),
	}
}

// SealOnce seals up to BatchLimit unsealed transactions for ledger l,
// creating a Checkpoint if CheckpointEvery or CheckpointMaxAge is reached.
func (s *Sealer) SealOnce(ctx context.Context, l core.LedgerID) (int, *core.Checkpoint, error) {
	j := s.store.Journal()
	unsealed, err := j.UnsealedTransactions(ctx, l, s.cfg.BatchLimit)
	if err != nil || len(unsealed) == 0 {
		return 0, nil, err
	}

	latest, err := j.LatestRecord(ctx, l)
	if err != nil {
		return 0, nil, err
	}

	var seq uint64
	var prevHash []byte
	if latest == nil {
		seq = 0
		prevHash = GenesisHash(l)
	} else {
		seq = latest.Seq
		prevHash = latest.ChainHash
	}

	now := s.cfg.Clock.Now()
	records := make([]core.JournalRecord, 0, len(unsealed))
	var checkpoint *core.Checkpoint

	for _, tx := range unsealed {
		seq++
		th := TxHash(tx)
		ch := ChainHash(prevHash, seq, th)
		rec := core.JournalRecord{
			Ledger:    l,
			Seq:       seq,
			TxID:      tx.ID,
			TxHash:    th,
			ChainHash: ch,
			SealedAt:  now,
		}
		records = append(records, rec)
		prevHash = ch

		if seq%s.cfg.CheckpointEvery == 0 {
			cp := core.Checkpoint{
				Ledger:    l,
				Seq:       seq,
				ChainHash: bytes.Clone(ch),
				CreatedAt: now,
			}
			checkpoint = &cp
			s.lastCheckpoint[l] = now
		}
	}

	// Also emit a checkpoint if CheckpointMaxAge has elapsed or this is the first seal batch
	if checkpoint == nil && len(records) > 0 {
		lastTime := s.lastCheckpoint[l]
		if lastTime.IsZero() || now.Sub(lastTime) >= s.cfg.CheckpointMaxAge {
			lastRec := records[len(records)-1]
			cp := core.Checkpoint{
				Ledger:    l,
				Seq:       lastRec.Seq,
				ChainHash: bytes.Clone(lastRec.ChainHash),
				CreatedAt: now,
			}
			checkpoint = &cp
			s.lastCheckpoint[l] = now
		}
	}

	if err := j.AppendRecords(ctx, records, checkpoint); err != nil {
		return 0, nil, err
	}

	// Emit journal.checkpoint outbox event if a checkpoint was produced
	if checkpoint != nil {
		tx, err := s.store.Begin(ctx)
		if err == nil {
			payload, _ := json.Marshal(map[string]any{
				"ledger":     checkpoint.Ledger,
				"seq":        checkpoint.Seq,
				"chain_hash": hex.EncodeToString(checkpoint.ChainHash),
				"created_at": checkpoint.CreatedAt,
			})
			_ = tx.AppendOutbox(ctx, []events.Event{{
				ID:        s.cfg.IDGen.NewID(now),
				Ledger:    l,
				Type:      events.TypeJournalCheckpoint,
				Payload:   payload,
				CreatedAt: now,
			}})
			_ = tx.Commit(ctx)
		}
	}

	return len(records), checkpoint, nil
}

// VerificationReport summarizes a VerifyChain run.
type VerificationReport struct {
	Ledger            core.LedgerID `json:"ledger"`
	VerifiedRecords   uint64        `json:"verified_records"`
	VerifiedCheckpts  int           `json:"verified_checkpoints"`
	LatestChainHash   string        `json:"latest_chain_hash"`
}

// VerifyChain recomputes every tx_hash and chain_hash from genesis for ledger l
// and verifies them against stored journal records and checkpoints (Invariant I8).
func VerifyChain(ctx context.Context, store storage.Store, l core.LedgerID) (*VerificationReport, error) {
	j := store.Journal()
	checkpoints, err := j.Checkpoints(ctx, l)
	if err != nil {
		return nil, err
	}
	cpMap := make(map[uint64][]byte, len(checkpoints))
	for _, cp := range checkpoints {
		cpMap[cp.Seq] = cp.ChainHash
	}

	prevHash := GenesisHash(l)
	var expectedSeq uint64
	var afterSeq uint64
	verifiedCPs := 0

	for {
		recs, err := j.Records(ctx, l, afterSeq, 500)
		if err != nil {
			return nil, err
		}
		if len(recs) == 0 {
			break
		}
		for _, r := range recs {
			expectedSeq++
			if r.Seq != expectedSeq {
				return nil, fmt.Errorf("journal sequence gap on ledger %s: expected seq %d, got %d", l, expectedSeq, r.Seq)
			}
			tx, err := store.Transaction(ctx, r.TxID)
			if err != nil {
				return nil, fmt.Errorf("missing transaction %s at journal seq %d: %w", r.TxID, r.Seq, err)
			}
			computedTxHash := TxHash(tx)
			if !bytes.Equal(computedTxHash, r.TxHash) {
				return nil, fmt.Errorf("tx_hash mismatch at seq %d (tx %s): stored %x != computed %x",
					r.Seq, r.TxID, r.TxHash, computedTxHash)
			}
			computedChainHash := ChainHash(prevHash, r.Seq, computedTxHash)
			if !bytes.Equal(computedChainHash, r.ChainHash) {
				return nil, fmt.Errorf("chain_hash mismatch at seq %d (tx %s): stored %x != computed %x",
					r.Seq, r.TxID, r.ChainHash, computedChainHash)
			}
			if cpHash, hasCP := cpMap[r.Seq]; hasCP {
				if !bytes.Equal(cpHash, computedChainHash) {
					return nil, fmt.Errorf("checkpoint mismatch at seq %d: checkpoint %x != chain %x",
						r.Seq, cpHash, computedChainHash)
				}
				verifiedCPs++
			}
			prevHash = computedChainHash
			afterSeq = r.Seq
		}
	}

	if verifiedCPs != len(checkpoints) {
		return nil, fmt.Errorf("checkpoint count mismatch: verified %d of %d checkpoints", verifiedCPs, len(checkpoints))
	}

	return &VerificationReport{
		Ledger:           l,
		VerifiedRecords:  expectedSeq,
		VerifiedCheckpts: verifiedCPs,
		LatestChainHash:  hex.EncodeToString(prevHash),
	}, nil
}
