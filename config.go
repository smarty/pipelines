package pipelines

type config struct {
	logger Logger
	groups []*group
}

func (this *config) apply(options ...option) {
	for _, option := range append(Options.defaults(), options...) {
		if option != nil {
			option(this)
		}
	}
}

type option func(*config)

var Options options

type options struct{}

func (options) Logger(logger Logger) option {
	return func(c *config) { c.logger = logger }
}

func (options) StationGroup(options ...groupOption) option {
	return func(c *config) {
		group := new(group)
		for _, option := range GroupOptions.defaults(options...) {
			option(group)
		}
		if len(group.stations) > 0 {
			c.groups = append(c.groups, group)
		}
	}
}

func (options) defaults(options ...option) []option {
	return append([]option{
		Options.Logger(nop{}),
	}, options...)
}

type nop struct{}

func (nop) Printf(_ string, _ ...any) {}

type groupOption func(*group)

type groupOptions struct{}

var GroupOptions groupOptions

// Stations provides stations to the group. A group with no stations is considered inert and will thus be discarded.
// Providing more than one station results in a fan-out/fan-in (see the README.md for additional details).
func (groupOptions) Stations(stations ...Station) groupOption {
	return func(g *group) { g.stations = stations }
}

// BufferedOutput ensures that the associated group's stations emit to a buffered channel.
// The provided capacity will be set to 1 if a lower value is provided.
// A capacity of 1 is equivalent to an unbuffered channel.
func (groupOptions) BufferedOutput(capacity int) groupOption {
	return func(g *group) { g.bufferCapacity = max(unbufferedChannelCapacity, capacity) }
}

// SendViaSelect (with a non-nil callback) employs a channel send operation as part of a select statement. The default
// case passes items to the provided callback when the input channel to the next station is full.
// See https://go.dev/ref/spec#Select_statements for technical details.
// When provided a nil callback (the default) a traditional channel send operation is used, which will block when the
// input channel to the next station is full.
// (WARNING: May cause the entire pipeline to hang/deadlock in the case of a station with an errant infinite loop!)
func (groupOptions) SendViaSelect(callback func(any)) groupOption {
	return func(g *group) { g.sendViaSelectCallback = callback }
}

func (groupOptions) defaults(options ...groupOption) []groupOption {
	return append([]groupOption{
		GroupOptions.BufferedOutput(unbufferedChannelCapacity),
		GroupOptions.SendViaSelect(nil),
	}, options...)
}

const unbufferedChannelCapacity = 1
