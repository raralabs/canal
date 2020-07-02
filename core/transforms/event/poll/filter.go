package poll

import (
	"sync"
)

// FilterEvent is a filter event, that should be polled to see if it has been
// triggered.
type FilterEvent struct {
	event EventFilter

	muRunning *sync.Mutex
	running   bool
}

// NewFilterEvent creates a new filter on the basis of provided event filter
// function.
func NewFilterEvent(filt EventFilter) *FilterEvent {
	return &FilterEvent{event: filt, muRunning: &sync.Mutex{}}
}

// start starts the event.
func (fe *FilterEvent) Start() {
	fe.muRunning.Lock()
	fe.running = true
	fe.muRunning.Unlock()
}

// Stop stops the event.
func (fe *FilterEvent) Stop() {
	fe.muRunning.Lock()
	fe.running = false
	fe.muRunning.Unlock()
}

// ValueType gives the type of the event.
func (fe *FilterEvent) Type() EventType { return FILTER }

// Triggered checks what the provided event filter function evaluates to and
// returns it.
func (fe *FilterEvent) Triggered(m map[string]interface{}) bool {

	// fmt.Println(m)

	fe.muRunning.Lock()
	defer fe.muRunning.Unlock()
	if !fe.running {
		return false
	}

	return fe.event(m)
}

func (fe *FilterEvent) Reset() {

}
