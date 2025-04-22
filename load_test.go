package pipelines_test

import (
	"sync/atomic"
	"testing"

	"github.com/smarty/pipelines"
)

func TestLoad(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	var group1 []pipelines.Station
	for range 1024 {
		group1 = append(group1, NewLoadTestStation())
	}

	var group2 []pipelines.Station
	for range 8 {
		group2 = append(group2, NewLoadTestStation())
	}

	const totalItems = 10_000_000
	input := make(chan any)
	go func() {
		defer close(input)
		for range totalItems {
			input <- struct{}{}
		}
	}()

	station3 := NewLoadTestStation()
	station4 := NewLoadTestFinalStation(t, totalItems)

	listener := pipelines.New(input,
		pipelines.Options.Logger(&TLogger{T: t}),
		pipelines.Options.StationGroup(
			pipelines.GroupOptions.Stations(group1...),
			pipelines.GroupOptions.SendViaSelect(station4.backdoor),
		),
		pipelines.Options.StationGroup(
			pipelines.GroupOptions.Stations(group2...),
			pipelines.GroupOptions.BufferedOutput(1000),
		),
		pipelines.Options.StationGroup(
			pipelines.GroupOptions.Stations(station3),
			pipelines.GroupOptions.BufferedOutput(1000),
		),
		pipelines.Options.StationGroup(
			pipelines.GroupOptions.Stations(station4),
		),
	)
	listener.Listen()

	for _, station := range append(group1, group2...) {
		if station.(*LoadTestStation).count == 0 {
			t.Error("a fanned-out station handled 0 items")
		}
	}
}

/////////////////////////////

type LoadTestStation struct {
	count int
}

func NewLoadTestStation() *LoadTestStation {
	return &LoadTestStation{}
}

func (this *LoadTestStation) Do(input any, output func(any)) {
	this.count++
	output(input)
}

//////////////////////////////////

type LoadTestFinalStation struct {
	t              *testing.T
	backdoorCount  *atomic.Int64
	processedCount *atomic.Int64
	expectedCount  int64
}

func NewLoadTestFinalStation(t *testing.T, expectedCount int) *LoadTestFinalStation {
	return &LoadTestFinalStation{
		t:              t,
		backdoorCount:  new(atomic.Int64),
		processedCount: new(atomic.Int64),
		expectedCount:  int64(expectedCount),
	}
}
func (this *LoadTestFinalStation) Do(input any, output func(any)) {
	actual := this.processedCount.Add(1)
	this.progress(actual)
}
func (this *LoadTestFinalStation) Finalize(_ func(any)) {
	processed := this.processedCount.Load()
	backdoor := this.backdoorCount.Load()
	this.t.Logf("Station finished after processing %d items (%d items were discarded)", processed, backdoor)
	if processed+backdoor != this.expectedCount {
		this.t.Logf("expected %d total items, got %d", this.expectedCount, processed+backdoor)
	}
}
func (this *LoadTestFinalStation) backdoor(any) {
	_ = this.backdoorCount.Add(1)
}

func (this *LoadTestFinalStation) progress(actual int64) {
	backdoor := this.backdoorCount.Load()
	if (actual+backdoor)%100_000 == 0 {
		this.t.Logf("progress: %d/%d (%%%d) backdoor: %d/%d (%%%d)",
			actual,
			this.expectedCount,
			int(float64(actual)/float64(this.expectedCount)*100),
			backdoor,
			this.expectedCount,
			int(float64(backdoor)/float64(this.expectedCount)*100),
		)
	}
}
