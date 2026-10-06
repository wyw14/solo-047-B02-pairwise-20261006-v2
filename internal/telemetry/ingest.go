package telemetry

import (
	"context"
	"errors"
	"fmt"
	"time"
)

type ProjectionSink interface {
	ApplySample(context.Context, Sample) error
}

type Ingestor struct {
	repository *Repository
	sinks      []ProjectionSink
	now        func() time.Time
}

func NewIngestor(repository *Repository, now func() time.Time, sinks ...ProjectionSink) *Ingestor {
	return &Ingestor{repository: repository, sinks: sinks, now: now}
}

func (i *Ingestor) Apply(ctx context.Context, sample Sample) error {
	if sample.ReceivedAt.IsZero() {
		sample.ReceivedAt = i.now().UTC()
	}
	if err := i.repository.Add(sample); err != nil {
		// A sequence conflict keeps the first fact and is already recorded by
		// the repository: never project the rejected value at sinks.
		if errors.Is(err, ErrSequenceConflict) {
			return err
		}
		return fmt.Errorf("store telemetry sample: %w", err)
	}
	for _, sink := range i.sinks {
		if err := sink.ApplySample(ctx, sample); err != nil {
			return fmt.Errorf("project telemetry sample: %w", err)
		}
	}
	return nil
}
