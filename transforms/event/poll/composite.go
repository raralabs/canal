package poll

import (
	"log"
	"strings"
)

// A CompositeEvent is an event that is made up of multiple events and related
// by certain operators like 'and', 'or'.
type CompositeEvent struct {
	events   []Event
	operator string

	tmrTrg bool
}

// NewCompositeEvent creates a new composite event on the basis of the provided
// operator and the events.
func NewCompositeEvent(operator string, events ...Event) *CompositeEvent {
	evs := make([]Event, len(events))
	for i, ev := range events {
		evs[i] = ev
	}

	op := strings.ToLower(strings.TrimSpace(operator))

	return &CompositeEvent{operator: op, events: evs}
}

// start starts all the events associated with the composite event.
func (ce *CompositeEvent) Start() {
	for _, e := range ce.events {
		e.Start()
	}
}

// Stop stops all the events associated with the composite event.
func (ce *CompositeEvent) Stop() {
	for _, e := range ce.events {
		e.Stop()
	}
}

// ValueType returns the type of the event.
func (ce *CompositeEvent) Type() EventType { return COMPOSITE }

// Triggered checks if all the events have been triggered and responds according
// to the operator provided.
func (ce *CompositeEvent) Triggered(m map[string]interface{}) bool {

	// If there are no events, always return false if asked for trigger.
	if len(ce.events) == 0 {
		return false
	}

	// Collect triggers from all the associated events.
	trgs := make([]bool, len(ce.events))
	for i, ev := range ce.events {
		trgs[i] = ev.Triggered(m)
	}

	// Return the triggered value of the first event if it is the only event
	// in the composite event.
	if len(ce.events) == 1 {
		return trgs[0]
	}

	return ce.processTriggers(trgs)
}

// Reset resets all the events associated with the composite event.
func (ce *CompositeEvent) Reset() {
	for _, e := range ce.events {
		e.Reset()
	}
}

// AddEvents add events to be checked for triggers to the composite event.
func (ce *CompositeEvent) AddEvents(ev ...Event) {
	ce.events = append(ce.events, ev...)
}

// processTriggers processes the triggers according to the logical operator
// stored by the CompositeEvent and the given initial condition if given.
func (ce *CompositeEvent) processTriggers(trgs []bool) bool {

	orInitial := false
	andInitial := true

	// Perform logical operations according to the operator provided.
	switch ce.operator {
	case "and":
		out := andInitial
		for _, v := range trgs {
			out = out && v
			// Return false as soon as the out becomes false.
			if !out {
				return false
			}
		}
		return out
	case "or":
		out := orInitial
		for _, v := range trgs {
			out = out || v
			// Return true as soon as the out becomes true.
			if out {
				return true
			}
		}
		return out
	default:
		log.Fatalf("Unsupported Operator: %v", ce.operator)
	}

	return false
}
