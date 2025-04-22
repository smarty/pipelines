#### SMARTY DISCLAIMER: Subject to the terms of the associated license agreement, this software is freely available for your use. This software is FREE, AS IN PUPPIES, and is a gift. Enjoy your new responsibility. This means that while we may consider enhancement requests, we may or may not choose to entertain requests at our sole and absolute discretion.

# github.com/smarty/pipelines

A flexible yet minimal framework for setting up concurrent assembly lines in Go.

The assembly line is essentially several groups of 'stations' chained together with channels. Each group consists of one or more 'stations', each reading off of the same input channel, which the previous station is responsible to send values on (and eventually close). When a group consists of multiple stations, the [fan-out/fan-in algorithm described on the Go blog](https://go.dev/blog/pipelines).

Each 'station' implements the following (very vague) interface:

```go
type Station interface {
	Do(input any, output func(any))
}
```

- The `input` is a value received from that station group's input channel.
  - Generally, a type switch is used to determine what to do with the value.
- The `output` func is a send operation on that station's output channel.
  - It is the caller's responsibility to ensure that a station group 'downstream' can handle the value being sent.
  - Generally it is an oversight for values to be sent by the last station and any such values will simply be logged by the library.
- Multiple stations in a group will result in a fan-out/fan-in (as referenced above). 

Stations that also implement the following interface have one last shot at sending values before shutting down:

```go
type Finalizer interface {
	Finalize(output func(any))
}
```

See the test cases for examples of actual pipelines.
