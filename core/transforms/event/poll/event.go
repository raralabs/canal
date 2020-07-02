package poll

type EventType uint8

// These constants determine the type of the events.
const (
	FILTER EventType = iota + 1
	TIMER

	COMPOSITE
)

// An EventFilter represents an event filter type
type EventFilter func(map[string]interface{}) bool

// Event is a event that should be polled to determine if it has been
// triggered.
type Event interface {
	// ValueType returns the type of the event.
	Type() EventType

	// start starts the checking for the events.
	Start()

	// Stop stops the checking for the new events. Call to Triggered always
	// returns false if the Event has been stopped.
	Stop()

	// Triggered polls the events and tells if the event has been triggered.
	// A map containing states is passed to the event while checking for
	// triggers.
	Triggered(map[string]interface{}) bool

	// Reset resets the variable exposed by the event. This can be used for
	// resetting some synchronizing variables. It should be called after
	// calling Triggered multiple times, if needed.
	Reset()
}
