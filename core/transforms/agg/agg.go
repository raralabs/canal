package agg

import (

	"github.com/raralabs/canal/core/message"
	"github.com/raralabs/canal/core/message/content"
	"github.com/raralabs/canal/core/pipeline"
)

type Aggregator struct {
	table *Table                                                                                    // The table that holds all the aggregator's info
	after func(message.Msg, pipeline.IProcessorForExecutor, []content.IContent, []content.IContent) // This function is called after execute on each message is called
}

// NewAggregator creates a new aggregator with the provided events, aggregators
// and the groups and returns it.
func NewAggregator(aggs []IAggFuncTemplate,
	after func(message.Msg, pipeline.IProcessorForExecutor, []content.IContent, []content.IContent),
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

func (ag *Aggregator) AggFunc(m message.Msg, s *struct{}) ([]content.IContent, []content.IContent, error) {

	var contents, pContent content.IContent
	if m.Content() != nil {
		contents = m.Content().Copy()
	}
	if m.PrevContent() != nil {
		pContent = m.PrevContent().Copy()
	}

	return ag.table.Insert(contents, pContent)
}

func (ag *Aggregator) Function() pipeline.Executor {
	var s struct{}
	return NewOperator(s, ag.AggFunc, ag.after)
}

func (ag *Aggregator) Entry(group string) content.IContent {
	return ag.table.Entry(group)
}

func (ag *Aggregator) Entries() []content.IContent {
	return ag.table.Entries()
}
