package agg

import (
	"github.com/raralabs/canal/core/message"
	"github.com/raralabs/canal/core/pipeline"
)

type Aggregator struct {
	table *Table                                                                                              // The table that holds all the aggregator's info
	after func(message.Msg, pipeline.IProcessorForExecutor, []*message.OrderedContent, []*message.OrderedContent) // This function is called after execute on each message is called
}

// NewAggregator creates a new aggregator with the provided events, aggregators
// and the groups and returns it.
func NewAggregator(aggs []IAggFuncTemplate,
	after func(message.Msg, pipeline.IProcessorForExecutor, []*message.OrderedContent, []*message.OrderedContent),
	groupBy ...string) *Aggregator {

	ag := &Aggregator{
		after: after,
	}

	tbl := NewTable(aggs, groupBy...)
	ag.table = tbl

	return ag
}

// Reset resets the aggregator.
func (ag *Aggregator) Reset() {
	ag.table.Reset()
}

func (ag *Aggregator) AggFunc(m message.Msg, s *struct{}) ([]*message.OrderedContent, []*message.OrderedContent, error) {

	content := m.Content()
	prevContent := m.PrevContent()
	return ag.table.Insert(content, prevContent)
}

func (ag *Aggregator) Function() pipeline.Executor {
	var s struct{}
	return NewOperator(s, ag.AggFunc, ag.after)
}

func (ag *Aggregator) Entry(group string) *message.OrderedContent {
	return ag.table.Entry(group)
}

func (ag *Aggregator) Entries() []*message.OrderedContent {
	return ag.table.Entries()
}
