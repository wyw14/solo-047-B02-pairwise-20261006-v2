package telemetry

import (
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"
)

type Kind string

const (
	KindOpticalPower Kind = "optical_power"
	KindErrorRate    Kind = "error_rate"
	KindCurrent      Kind = "current"
	KindTemperature  Kind = "temperature"
	KindHeartbeat    Kind = "heartbeat"
)

// ErrSequenceConflict is returned when a sample reuses the sequence number of
// an already accepted sample for the same asset, generation and kind but
// carries a different value. The first accepted reading is retained.
var ErrSequenceConflict = errors.New("telemetry sample sequence conflict")

type Sample struct {
	ID         string    `json:"id"`
	AssetID    string    `json:"asset_id"`
	Kind       Kind      `json:"kind"`
	Value      float64   `json:"value"`
	Unit       string    `json:"unit"`
	Sequence   uint64    `json:"sequence"`
	Generation uint64    `json:"generation"`
	ObservedAt time.Time `json:"observed_at"`
	ReceivedAt time.Time `json:"received_at"`
}

func (s Sample) Validate() error {
	if s.ID == "" || s.AssetID == "" || s.Kind == "" {
		return errors.New("sample identity, asset and kind are required")
	}
	if s.Sequence == 0 || s.ObservedAt.IsZero() {
		return errors.New("sample sequence and observed time are required")
	}
	return nil
}

// Conflict records a rejected sample that reused the sequence of an accepted
// sample for the same asset, generation and kind but reported a different
// reading. The existing sample remains the source of truth.
type Conflict struct {
	AssetID    string    `json:"asset_id"`
	Kind       Kind      `json:"kind"`
	Generation uint64    `json:"generation"`
	Sequence   uint64    `json:"sequence"`
	Existing   Sample    `json:"existing"`
	Duplicate  Sample    `json:"duplicate"`
	RecordedAt time.Time `json:"recorded_at"`
}

// sampleKey identifies a unique reading slot: a sequence number is scoped to
// one asset, topology generation and sample kind.
type sampleKey struct {
	generation uint64
	kind       Kind
	sequence   uint64
}

func keyOf(sample Sample) sampleKey {
	return sampleKey{generation: sample.Generation, kind: sample.Kind, sequence: sample.Sequence}
}

func keyBefore(a, b sampleKey) bool {
	if a.generation != b.generation {
		return a.generation < b.generation
	}
	if a.kind != b.kind {
		return a.kind < b.kind
	}
	return a.sequence < b.sequence
}

// sameReading reports whether two samples with the same key carry the same
// field reading, i.e. the second one is an idempotent redelivery.
func sameReading(a, b Sample) bool {
	return a.Value == b.Value && a.Unit == b.Unit
}

type Repository struct {
	mu        sync.RWMutex
	samples   map[string][]Sample
	conflicts map[string][]Conflict
}

func NewRepository() *Repository {
	return &Repository{
		samples:   make(map[string][]Sample),
		conflicts: make(map[string][]Conflict),
	}
}

func (r *Repository) Add(sample Sample) error {
	_, err := r.Accept(sample)
	return err
}

// Accept stores the sample and reports whether it was a new reading that
// downstream projections should observe. A false, nil-result means an
// idempotent redelivery of an already accepted reading. Samples that reuse a
// sequence with a different value are rejected and recorded as conflicts; the
// first accepted reading is always retained.
func (r *Repository) Accept(sample Sample) (bool, error) {
	if err := sample.Validate(); err != nil {
		return false, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	key := keyOf(sample)
	items := r.samples[sample.AssetID]
	position := sort.Search(len(items), func(index int) bool {
		return !keyBefore(keyOf(items[index]), key)
	})
	if position < len(items) && keyOf(items[position]) == key {
		existing := items[position]
		if sameReading(existing, sample) {
			return false, nil
		}
		r.conflicts[sample.AssetID] = append(r.conflicts[sample.AssetID], Conflict{
			AssetID: sample.AssetID, Kind: sample.Kind, Generation: sample.Generation,
			Sequence: sample.Sequence, Existing: existing, Duplicate: sample, RecordedAt: sample.ReceivedAt,
		})
		return false, fmt.Errorf("%w: %s %s generation=%d sequence=%d keeps value %v, rejected conflicting value %v",
			ErrSequenceConflict, sample.AssetID, sample.Kind, sample.Generation, sample.Sequence, existing.Value, sample.Value)
	}
	items = append(items, Sample{})
	copy(items[position+1:], items[position:])
	items[position] = sample
	r.samples[sample.AssetID] = items
	return true, nil
}

func (r *Repository) History(assetID string) []Sample {
	r.mu.RLock()
	defer r.mu.RUnlock()
	items := r.samples[assetID]
	result := make([]Sample, len(items))
	copy(result, items)
	return result
}

// Conflicts returns the sequence conflicts recorded for an asset, oldest
// first. The first accepted reading of each conflicting sequence stays in
// History and Latest.
func (r *Repository) Conflicts(assetID string) []Conflict {
	r.mu.RLock()
	defer r.mu.RUnlock()
	recorded := r.conflicts[assetID]
	result := make([]Conflict, len(recorded))
	copy(result, recorded)
	return result
}

func (r *Repository) Latest(assetID string, kind Kind) (Sample, bool) {
	items := r.History(assetID)
	for index := len(items) - 1; index >= 0; index-- {
		if items[index].Kind == kind {
			return items[index], true
		}
	}
	return Sample{}, false
}
