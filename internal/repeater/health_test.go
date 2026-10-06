package repeater

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wyw14/cry-103/internal/telemetry"
)

type fakeIncidents struct {
	opened      int64
	resolutions map[string]uint64
}

func (f *fakeIncidents) OpenRepeaterIncident(string, uint64) error {
	atomic.AddInt64(&f.opened, 1)
	return nil
}

func (f *fakeIncidents) ResolveRepeaterIncident(id string, generation uint64) error {
	f.resolutions[id] = generation
	return nil
}

func (f *fakeIncidents) Active(string) (uint64, bool) { return 0, false }
func (f *fakeIncidents) LastResolution(string) uint64 { return 0 }

func temperatureSample(value float64, sequence uint64) telemetry.Sample {
	return telemetry.Sample{
		ID: "s", AssetID: "R07", Kind: telemetry.KindTemperature, Value: value, Unit: "C",
		Sequence: sequence, Generation: 1, ObservedAt: time.Date(2026, 10, 5, 1, 0, 0, 0, time.UTC),
	}
}

func TestHealthProjectorIgnoresReplayedSequence(t *testing.T) {
	asset, err := New("R07", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	incidents := &fakeIncidents{resolutions: map[string]uint64{}}
	projector := NewHealthProjector(incidents, 85)
	projector.Register(asset)
	ctx := context.Background()

	if err := projector.ApplySample(ctx, temperatureSample(90, 1)); err != nil {
		t.Fatal(err)
	}
	// Same sequence, lower value: must neither clear the latch nor overwrite
	// the field reading, and must not open a second incident.
	if err := projector.ApplySample(ctx, temperatureSample(40, 1)); err != nil {
		t.Fatal(err)
	}
	snapshot := asset.Snapshot()
	if snapshot.Health != HealthTripped || snapshot.TemperatureC != 90 || snapshot.LastSequence != 1 {
		t.Fatalf("replayed sequence changed projection: %+v", snapshot)
	}
	if got := atomic.LoadInt64(&incidents.opened); got != 1 {
		t.Fatalf("incident must open exactly once, opened %d times", got)
	}
}

func TestHealthProjectorConcurrentSameSequenceTripsOnce(t *testing.T) {
	asset, err := New("R07", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	incidents := &fakeIncidents{resolutions: map[string]uint64{}}
	projector := NewHealthProjector(incidents, 85)
	projector.Register(asset)
	ctx := context.Background()

	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_ = projector.ApplySample(ctx, temperatureSample(90, 1))
		}()
	}
	close(start)
	wg.Wait()

	snapshot := asset.Snapshot()
	if snapshot.Health != HealthTripped || snapshot.LastSequence != 1 {
		t.Fatalf("unexpected snapshot after concurrent delivery: %+v", snapshot)
	}
	if got := atomic.LoadInt64(&incidents.opened); got != 1 {
		t.Fatalf("incident must open exactly once under concurrency, opened %d times", got)
	}
}
