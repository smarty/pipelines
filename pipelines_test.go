package pipelines_test

import (
	"reflect"
	"sort"
	"sync/atomic"
	"testing"

	"github.com/smarty/pipelines"
)

func TestNoStations_AllValuesLogged(t *testing.T) {
	input := make(chan any)
	go func() {
		defer close(input)
		for x := range 10 {
			input <- x
		}
	}()
	logger := &TLogger{T: t}
	listener := pipelines.New(input,
		pipelines.Options.Logger(logger),
	)
	listener.Listen()

	if logger.count != 10 {
		t.Errorf("got %d log calls, should have 10", logger.count)
	}
}

// Test a somewhat interesting pipeline example, based on this Clojure threading macro example:
// https://clojuredocs.org/clojure.core/-%3E%3E#example-542692c8c026201cdc326a52
// (->> (range) (map #(* % %)) (filter even?) (take 10) (reduce +))  ; output: 1140
// Coincidentally, using github.com/mdw-go/funcy/ranger you can achieve the same result as follows:
// Reduce(op.Add, 0, Take(10, Filter(is.Even, Map(op.Square, RangeOpen(0, 1)))))
func TestPipelineExample(t *testing.T) {
	input := make(chan any)
	go func() {
		defer close(input)
		for x := range 50 {
			input <- x
		}
	}()

	sum := new(atomic.Int64)
	closed := new(atomic.Int64)
	var (
		squares = NewSquares()
		evens   = NewEvens()
		firstN  = NewFirstN(10)
		sums    = []pipelines.Station{
			NewSum(sum, closed),
			NewSum(sum, closed),
			NewSum(sum, closed),
			NewSum(sum, closed),
			NewSum(sum, closed),
		}
		catchAll = NewCatchAll()
	)
	listener := pipelines.New(input,
		pipelines.Options.Logger(&TLogger{T: t}),
		pipelines.Options.StationGroup(pipelines.GroupOptions.Stations(squares)),
		pipelines.Options.StationGroup(pipelines.GroupOptions.Stations(evens)),
		pipelines.Options.StationGroup(pipelines.GroupOptions.Stations()), // no stations, will be ignored
		pipelines.Options.StationGroup(pipelines.GroupOptions.Stations(firstN)),
		pipelines.Options.StationGroup(pipelines.GroupOptions.Stations(sums...)), // fan-out
		pipelines.Options.StationGroup(pipelines.GroupOptions.Stations(catchAll)),
	)

	listener.Listen()

	const expected = 1140
	if total := sum.Load(); total != expected {
		t.Errorf("Expected %d, got %d", expected, total)
	}
	if final := int(closed.Load()); final != len(sums) {
		t.Errorf("Expected %d, got %d", len(sums), final)
	}
	sort.Ints(catchAll.final)
	if !reflect.DeepEqual(catchAll.final, []int{1, 2, 3, 4, 5}) {
		t.Errorf("Expected %d, got %d", []int{1, 2, 3, 4, 5}, catchAll.final)
	}
}

type TLogger struct {
	*testing.T
	count int
}

func (this *TLogger) Printf(format string, args ...any) {
	this.Helper()
	this.Logf(format, args...)
	this.count++
}

///////////////////////////////

type Squares struct{}

func NewSquares() pipelines.Station {
	return &Squares{}
}

func (this *Squares) Do(input any, output func(any)) {
	switch input := input.(type) {
	case int:
		output(input * input)
	}
}

///////////////////////////////

type Evens struct{}

func NewEvens() pipelines.Station {
	return &Evens{}
}

func (this *Evens) Do(input any, output func(any)) {
	switch input := input.(type) {
	case int:
		if input%2 == 0 {
			output(input)
		}
	}
}

///////////////////////////////

type FirstN struct {
	N       *atomic.Int64
	handled *atomic.Int64
}

func NewFirstN(n int64) pipelines.Station {
	N := new(atomic.Int64)
	N.Add(n)
	return &FirstN{N: N, handled: new(atomic.Int64)}
}

func (this *FirstN) Do(input any, output func(any)) {
	if this.handled.Load() >= this.N.Load() {
		return
	}
	output(input)
	this.handled.Add(1)
}

///////////////////////////////

type Sum struct {
	sum       *atomic.Int64
	finalized *atomic.Int64
}

func NewSum(sum, finalized *atomic.Int64) pipelines.Station {
	return &Sum{sum: sum, finalized: finalized}
}

func (this *Sum) Do(input any, output func(any)) {
	switch input := input.(type) {
	case int:
		this.sum.Add(int64(input))
		output(input)
	}
}

func (this *Sum) Finalize(output func(any)) {
	output(this.finalized.Add(1))
}

///////////////////////////////

type CatchAll struct {
	final []int
}

func NewCatchAll() *CatchAll {
	return &CatchAll{}
}

func (this *CatchAll) Do(input any, _ func(any)) {
	switch input := input.(type) {
	case int64:
		this.final = append(this.final, int(input))
	}
}
