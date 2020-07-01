package transforms

import (
	"github.com/raralabs/canal/core/message"
	"github.com/raralabs/canal/core/pipeline"
	"github.com/raralabs/canal/ext/transforms/agg"
	"github.com/raralabs/canal/ext/transforms/base_transforms"
	"github.com/raralabs/canal/ext/transforms/event/poll"
)

type Aggregator struct {
	table *agg.Table           // The table that holds all the aggregator's info
	ev    *poll.CompositeEvent // The event that triggers the aggregator output
}

// NewAggregator creates a new aggregator with the provided events, aggregators
// and the groups and returns it.
func NewAggregator(event poll.Event, aggs []agg.Aggregator, groupBy ...string) *Aggregator {

	ag := &Aggregator{}

	if ev, ok := event.(*poll.CompositeEvent); ok {
		ag.ev = ev
	} else {
		ag.ev = poll.NewCompositeEvent("or", event)
	}

	tbl := agg.NewTable(aggs, groupBy...)
	ag.table = tbl

	return ag
}

// AddEvents adds events to the Aggregator.
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

// Fulfilling the functions for Aggregator to act as an aggFunc

func (ag *Aggregator) toMessage(s *struct{}) []*message.MsgContent {

	var msgs []*message.MsgContent
	msgVals := ag.table.Messages()

	if len(msgVals) != 0 {
		for _, mv := range msgVals {
			// Check if the event has been triggered for the current messageValue
			if ag.ev.Triggered(mv.Values()) {
				msgs = append(msgs, &mv)
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
	return base_transforms.NewAggOperator(s, ag.toMessage, ag.aggFunc)
}
