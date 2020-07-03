package agg

import (
	"github.com/raralabs/canal/core/message"
	"github.com/raralabs/canal/core/pipeline"
	"github.com/raralabs/canal/core/transforms/event/poll"
)

type Aggregator struct {
	table *Table                                                                       // The table that holds all the aggregator's info
	ev    *poll.CompositeEvent                                                         // The event that triggers the aggregator output
	after func(message.Msg, pipeline.IProcessorForExecutor, []message.MsgContent) bool // This function is called after execute on each message is called
}

// NewAggregator creates a new aggregator with the provided events, aggregators
// and the groups and returns it.
func NewAggregator(event poll.Event, aggs []IAggregator, after func(message.Msg, pipeline.IProcessorForExecutor, []message.MsgContent) bool, groupBy ...string) *Aggregator {

	ag := &Aggregator{
		after: after,
	}

	if ev, ok := event.(*poll.CompositeEvent); ok {
		ag.ev = ev
	} else {
		ag.ev = poll.NewCompositeEvent("or", event)
	}

	tbl := NewTable(aggs, groupBy...)
	ag.table = tbl

	return ag
}

// AddEvents adds events to the IAggregator.
func (ag *Aggregator) AddEvents(events ...poll.Event) {
	ag.ev.AddEvents(events...)
}

// start starts the aggregator
func (ag *Aggregator) Start() {
	ag.ev.Start()
}

// Reset resets the aggregator.
func (ag *Aggregator) Reset() {
	ag.table.Reset()
}

// Fulfilling the functions for IAggregator to act as an aggFunc

func (ag *Aggregator) toMessage(s *struct{}) []message.MsgContent {

	var msgs []message.MsgContent
	msgVals := ag.table.Messages()

	if len(msgVals) != 0 {
		for _, mv := range msgVals {
			// Check if the event has been triggered for the current messageValue
			if ag.ev.Triggered(mv.Values()) {
				msgs = append(msgs, mv)
			}
		}
	}
	ag.ev.Reset()

	return msgs
}

func (ag *Aggregator) aggFunc(m message.Msg, s *struct{}) (bool, error) {

	content := m.Content()
	ag.table.Insert(&content)
	return true, nil
}

func (ag *Aggregator) Function() pipeline.Executor {
	var s struct{}
	return NewOperator(s, ag.toMessage, ag.aggFunc, ag.after)
}
