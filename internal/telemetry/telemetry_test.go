package telemetry

import (
	"context"
	"errors"
	"sync"
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

func sampleAt(id string, value float64, sequence uint64, observed time.Time) Sample {
	return Sample{
		ID: id, AssetID: "R07", Kind: KindTemperature, Value: value, Unit: "C",
		Sequence: sequence, Generation: 1, ObservedAt: observed, ReceivedAt: observed.Add(time.Second),
	}
}

func TestRepositoryRejectsConflictingSequenceKeepingFirstFact(t *testing.T) {
	repository := NewRepository()
	now := time.Now().UTC()
	first := sampleAt("s-1", 42.0, 7, now)
	later := sampleAt("s-2", 99.0, 7, now.Add(time.Second))

	if err := repository.Add(first); err != nil {
		t.Fatal(err)
	}
	err := repository.Add(later)
	if !errors.Is(err, ErrSequenceConflict) {
		t.Fatalf("expected ErrSequenceConflict, got %v", err)
	}

	history := repository.History("R07")
	if len(history) != 1 {
		t.Fatalf("conflicting sample must not be stored, got %d samples", len(history))
	}
	if latest, ok := repository.Latest("R07", KindTemperature); !ok || latest.Value != 42.0 || latest.ID != "s-1" {
		t.Fatalf("latest overwritten by conflict: %+v ok=%v", latest, ok)
	}

	conflicts := repository.Conflicts("R07")
	if len(conflicts) != 1 {
		t.Fatalf("expected one recorded conflict, got %d", len(conflicts))
	}
	record := conflicts[0]
	if record.Sequence != 7 || record.Generation != 1 || record.Kind != KindTemperature {
		t.Fatalf("conflict key mismatch: %+v", record)
	}
	if record.Existing.ID != "s-1" || record.Duplicate.ID != "s-2" {
		t.Fatalf("conflict must retain first fact and record the duplicate: %+v", record)
	}
}

func TestRepositoryAcceptsIdempotentRedelivery(t *testing.T) {
	repository := NewRepository()
	now := time.Now().UTC()
	first := sampleAt("s-1", 42.0, 7, now)
	redelivered := sampleAt("s-2", 42.0, 7, now)

	if err := repository.Add(first); err != nil {
		t.Fatal(err)
	}
	if accepted, err := repository.Accept(redelivered); err != nil || accepted {
		t.Fatalf("identical redelivery should be ignored without error, accepted=%v err=%v", accepted, err)
	}
	if len(repository.History("R07")) != 1 {
		t.Fatal("idempotent redelivery must not duplicate the reading")
	}
	if len(repository.Conflicts("R07")) != 0 {
		t.Fatal("identical redelivery is not a conflict")
	}
}

func TestRepositorySequenceUniquenessScopedByKindAndGeneration(t *testing.T) {
	repository := NewRepository()
	now := time.Now().UTC()
	samples := []Sample{
		sampleAt("t1", 42.0, 5, now),
		{ID: "c1", AssetID: "R07", Kind: KindCurrent, Value: 1.5, Unit: "A", Sequence: 5, Generation: 1, ObservedAt: now},
		{ID: "t2", AssetID: "R07", Kind: KindTemperature, Value: 43.0, Unit: "C", Sequence: 5, Generation: 2, ObservedAt: now},
	}
	for _, sample := range samples {
		if err := repository.Add(sample); err != nil {
			t.Fatalf("distinct kind/generation slots must coexist: %v", err)
		}
	}
	if len(repository.History("R07")) != 3 {
		t.Fatalf("expected three unique slots, got %d", len(repository.History("R07")))
	}
	if latest, ok := repository.Latest("R07", KindTemperature); !ok || latest.ID != "t2" {
		t.Fatalf("latest across generations should be t2, got %+v", latest)
	}
}

type countingSink struct{ applied int64 }

func (s *countingSink) ApplySample(context.Context, Sample) error {
	atomic.AddInt64(&s.applied, 1)
	return nil
}

func TestIngestorProjectsNewSamplesOnly(t *testing.T) {
	repository := NewRepository()
	sink := &countingSink{}
	ingestor := NewIngestor(repository, time.Now, sink)
	now := time.Now().UTC()

	newSample := sampleAt("s-1", 42.0, 7, now)
	redelivery := sampleAt("s-2", 42.0, 7, now)
	conflict := sampleAt("s-3", 99.0, 7, now.Add(time.Second))

	if err := ingestor.Apply(context.Background(), newSample); err != nil {
		t.Fatal(err)
	}
	if err := ingestor.Apply(context.Background(), redelivery); err != nil {
		t.Fatalf("idempotent redelivery should succeed: %v", err)
	}
	if err := ingestor.Apply(context.Background(), conflict); !errors.Is(err, ErrSequenceConflict) {
		t.Fatalf("conflicting apply should surface ErrSequenceConflict, got %v", err)
	}
	if got := atomic.LoadInt64(&sink.applied); got != 1 {
		t.Fatalf("projection must fire once for the first fact only, fired %d times", got)
	}
}

func TestIngestorConcurrentConflictProjectsOnce(t *testing.T) {
	repository := NewRepository()
	sink := &countingSink{}
	ingestor := NewIngestor(repository, time.Now, sink)
	now := time.Now().UTC()

	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			_ = ingestor.Apply(context.Background(), sampleAt("x", 42.0+float64(i), 7, now))
		}(i)
	}
	close(start)
	wg.Wait()

	if len(repository.History("R07")) != 1 {
		t.Fatalf("exactly one sample may win the sequence, got %d", len(repository.History("R07")))
	}
	if got := atomic.LoadInt64(&sink.applied); got != 1 {
		t.Fatalf("exactly one projection may fire, got %d", got)
	}
	if len(repository.Conflicts("R07")) != 15 {
		t.Fatalf("expected 15 recorded conflicts, got %d", len(repository.Conflicts("R07")))
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
