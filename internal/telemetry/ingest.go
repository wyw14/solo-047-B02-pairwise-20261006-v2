package telemetry

import (
	"context"
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
	accepted, err := i.repository.Accept(sample)
	if err != nil {
		return fmt.Errorf("store telemetry sample: %w", err)
	}
	if !accepted {
		// Idempotent redelivery of an already accepted reading: the first
		// fact stands and projections must not observe it a second time.
		return nil
	}
	for _, sink := range i.sinks {
		if err := sink.ApplySample(ctx, sample); err != nil {
			return fmt.Errorf("project telemetry sample: %w", err)
		}
	}
	return nil
}
