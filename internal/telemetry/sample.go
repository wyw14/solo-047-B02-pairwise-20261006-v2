package telemetry

import (
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"
)

// ErrSequenceConflict is returned when a sample carries the same
// (asset, kind, generation, sequence) identity as an earlier sample but a
// different value. The first fact is retained; callers can inspect the
// rejected delivery through Repository.Conflicts.
var ErrSequenceConflict = errors.New("telemetry sample conflicts with an earlier sample for the same sequence")

type Kind string

const (
	KindOpticalPower Kind = "optical_power"
	KindErrorRate    Kind = "error_rate"
	KindCurrent      Kind = "current"
	KindTemperature  Kind = "temperature"
	KindHeartbeat    Kind = "heartbeat"
)

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

// Conflict records a rejected delivery that reused a sequence number with a
// different value. The retained sample holds the first fact; the rejected
// sample captures what the relay attempted to overwrite it with.
type Conflict struct {
	AssetID    string    `json:"asset_id"`
	Kind       Kind      `json:"kind"`
	Generation uint64    `json:"generation"`
	Sequence   uint64    `json:"sequence"`
	Retained   Sample    `json:"retained"`
	Rejected   Sample    `json:"rejected"`
	RecordedAt time.Time `json:"recorded_at"`
}

type sampleKey struct {
	kind       Kind
	generation uint64
	sequence   uint64
}

type Repository struct {
	mu        sync.RWMutex
	samples   map[string][]Sample
	index     map[string]map[sampleKey]int
	conflicts map[string][]Conflict
}

func NewRepository() *Repository {
	return &Repository{
		samples:   make(map[string][]Sample),
		index:     make(map[string]map[sampleKey]int),
		conflicts: make(map[string][]Conflict),
	}
}

func (r *Repository) Add(sample Sample) error {
	if err := sample.Validate(); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	key := sampleKey{kind: sample.Kind, generation: sample.Generation, sequence: sample.Sequence}
	if index := r.index[sample.AssetID]; index != nil {
		if position, exists := index[key]; exists {
			existing := r.samples[sample.AssetID][position]
			// Identical redelivery is idempotent; retain the first fact silently.
			if existing.Value == sample.Value {
				return nil
			}
			r.conflicts[sample.AssetID] = append(r.conflicts[sample.AssetID], Conflict{
				AssetID: sample.AssetID, Kind: sample.Kind, Generation: sample.Generation,
				Sequence: sample.Sequence, Retained: existing, Rejected: sample,
				RecordedAt: latestNonZero(sample.ReceivedAt, existing.ReceivedAt),
			})
			return fmt.Errorf("%w: asset=%s kind=%s generation=%d sequence=%d retained=%.4g rejected=%.4g",
				ErrSequenceConflict, sample.AssetID, sample.Kind, sample.Generation, sample.Sequence,
				existing.Value, sample.Value)
		}
	}
	keyAt := len(r.samples[sample.AssetID])
	r.samples[sample.AssetID] = append(r.samples[sample.AssetID], sample)
	if r.index[sample.AssetID] == nil {
		r.index[sample.AssetID] = make(map[sampleKey]int)
	}
	r.index[sample.AssetID][key] = keyAt
	r.reindexLocked(sample.AssetID)
	return nil
}

// reindexLocked restores sequence ordering and refreshes key positions after
// an insertion. Sequence numbers are unique per (kind, generation), so no
// stable tie-break is needed.
func (r *Repository) reindexLocked(assetID string) {
	items := r.samples[assetID]
	sort.Slice(items, func(i, j int) bool {
		if items[i].Sequence != items[j].Sequence {
			return items[i].Sequence < items[j].Sequence
		}
		if items[i].Generation != items[j].Generation {
			return items[i].Generation < items[j].Generation
		}
		return items[i].Kind < items[j].Kind
	})
	index := r.index[assetID]
	for position, item := range items {
		index[sampleKey{kind: item.Kind, generation: item.Generation, sequence: item.Sequence}] = position
	}
}

func latestNonZero(times ...time.Time) time.Time {
	var latest time.Time
	for _, candidate := range times {
		if candidate.After(latest) {
			latest = candidate
		}
	}
	return latest
}

func (r *Repository) History(assetID string) []Sample {
	r.mu.RLock()
	defer r.mu.RUnlock()
	items := r.samples[assetID]
	result := make([]Sample, len(items))
	copy(result, items)
	return result
}

// Conflicts returns the rejected same-sequence deliveries for an asset,
// oldest first. Each entry preserves the first fact and the attempted value.
func (r *Repository) Conflicts(assetID string) []Conflict {
	r.mu.RLock()
	defer r.mu.RUnlock()
	items := r.conflicts[assetID]
	result := make([]Conflict, len(items))
	copy(result, items)
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
