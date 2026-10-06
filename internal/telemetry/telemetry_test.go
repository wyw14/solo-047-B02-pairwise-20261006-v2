package telemetry

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestRepositoryOrdersSamplesBySequence(t *testing.T) {
	repository := NewRepository()
	now := time.Now().UTC()
	for _, sequence := range []uint64{3, 1, 2} {
		if err := repository.Add(Sample{
			ID: "sample", AssetID: "R07", Kind: KindCurrent, Value: float64(sequence),
			Sequence: sequence, ObservedAt: now.Add(time.Duration(sequence) * time.Second),
		}); err != nil {
			t.Fatal(err)
		}
	}
	history := repository.History("R07")
	if len(history) != 3 || history[0].Sequence != 1 || history[2].Sequence != 3 {
		t.Fatalf("samples not ordered by sequence: %+v", history)
	}
}

func TestRepositoryKeepsFirstFactOnSequenceConflict(t *testing.T) {
	repository := NewRepository()
	now := time.Now().UTC()
	first := Sample{
		ID: "first", AssetID: "R07", Kind: KindTemperature, Value: 42, Unit: "C",
		Sequence: 7, Generation: 3, ObservedAt: now,
	}
	conflicting := first
	conflicting.ID = "second"
	conflicting.Value = 99
	conflicting.ObservedAt = now.Add(time.Second)
	conflicting.ReceivedAt = now.Add(2 * time.Second)
	if err := repository.Add(first); err != nil {
		t.Fatal(err)
	}
	err := repository.Add(conflicting)
	if !errors.Is(err, ErrSequenceConflict) {
		t.Fatalf("want ErrSequenceConflict, got %v", err)
	}
	history := repository.History("R07")
	if len(history) != 1 || history[0].ID != "first" {
		t.Fatalf("first fact must be retained, got %+v", history)
	}
	latest, ok := repository.Latest("R07", KindTemperature)
	if !ok || latest.Value != 42 || latest.ID != "first" {
		t.Fatalf("latest must not be overwritten by the conflict, got %+v ok=%v", latest, ok)
	}
	conflicts := repository.Conflicts("R07")
	if len(conflicts) != 1 {
		t.Fatalf("want one recorded conflict, got %+v", conflicts)
	}
	record := conflicts[0]
	if record.Retained.ID != "first" || record.Rejected.ID != "second" ||
		record.Retained.Value != 42 || record.Rejected.Value != 99 ||
		record.Sequence != 7 || record.Generation != 3 || record.RecordedAt.IsZero() {
		t.Fatalf("conflict record mismatch: %+v", record)
	}
}

func TestRepositoryTreatsIdenticalRedeliveryAsIdempotent(t *testing.T) {
	repository := NewRepository()
	now := time.Now().UTC()
	sample := Sample{
		ID: "first", AssetID: "R07", Kind: KindTemperature, Value: 42,
		Sequence: 7, Generation: 3, ObservedAt: now,
	}
	redelivery := sample
	redelivery.ID = "redelivery"
	if err := repository.Add(sample); err != nil {
		t.Fatal(err)
	}
	if err := repository.Add(redelivery); err != nil {
		t.Fatalf("identical redelivery must be accepted as a no-op, got %v", err)
	}
	if history := repository.History("R07"); len(history) != 1 || history[0].ID != "first" {
		t.Fatalf("redelivery must not duplicate the first fact: %+v", history)
	}
	if conflicts := repository.Conflicts("R07"); len(conflicts) != 0 {
		t.Fatalf("identical redelivery is not a conflict: %+v", conflicts)
	}
}

func TestRepositoryScopesUniquenessByGenerationAndKind(t *testing.T) {
	repository := NewRepository()
	now := time.Now().UTC()
	base := Sample{
		ID: "g3", AssetID: "R07", Kind: KindTemperature, Value: 42,
		Sequence: 7, Generation: 3, ObservedAt: now,
	}
	if err := repository.Add(base); err != nil {
		t.Fatal(err)
	}
	nextGeneration := base
	nextGeneration.ID = "g4"
	nextGeneration.Generation = 4
	nextGeneration.ObservedAt = now.Add(time.Second)
	if err := repository.Add(nextGeneration); err != nil {
		t.Fatalf("same sequence in a new generation is a new fact: %v", err)
	}
	otherKind := base
	otherKind.ID = "current"
	otherKind.Kind = KindCurrent
	if err := repository.Add(otherKind); err != nil {
		t.Fatalf("same sequence of a different kind is a new fact: %v", err)
	}
	if history := repository.History("R07"); len(history) != 3 {
		t.Fatalf("want three distinct facts, got %+v", history)
	}
	if conflicts := repository.Conflicts("R07"); len(conflicts) != 0 {
		t.Fatalf("unexpected conflicts: %+v", conflicts)
	}
}

type countingSink struct {
	calls atomic.Int32
}

func (s *countingSink) ApplySample(_ context.Context, _ Sample) error {
	s.calls.Add(1)
	return nil
}

func TestIngestorSkipsSinksOnSequenceConflict(t *testing.T) {
	repository := NewRepository()
	sink := &countingSink{}
	ingestor := NewIngestor(repository, func() time.Time { return time.Unix(100, 0).UTC() }, sink)
	now := time.Now().UTC()
	first := Sample{
		ID: "first", AssetID: "R07", Kind: KindTemperature, Value: 42,
		Sequence: 7, Generation: 3, ObservedAt: now,
	}
	conflicting := first
	conflicting.ID = "second"
	conflicting.Value = 99
	if err := ingestor.Apply(context.Background(), first); err != nil {
		t.Fatal(err)
	}
	if err := ingestor.Apply(context.Background(), conflicting); !errors.Is(err, ErrSequenceConflict) {
		t.Fatalf("want ErrSequenceConflict from ingestor, got %v", err)
	}
	if calls := sink.calls.Load(); calls != 1 {
		t.Fatalf("conflicting sample must not be projected, sink calls=%d", calls)
	}
}

func TestMuteManagerReleasesOnContextCancellation(t *testing.T) {
	manager := NewMuteManager(time.Now)
	manager.Acquire("mute-1", "S3", time.Second)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		manager.Run(ctx, "mute-1", time.Second)
		close(done)
	}()
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("mute renewal did not stop")
	}
	if manager.Muted("S3") {
		t.Fatal("mute remained active after cancellation")
	}
}
