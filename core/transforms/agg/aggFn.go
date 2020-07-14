package agg

import (
	"github.com/raralabs/canal/core/message"
	"github.com/raralabs/canal/core/pipeline"
	"log"
)

type Operator struct {
	name    string
	state   *struct{}
	aggFunc func(message.Msg, *struct{}) (*message.OrderedContent, *message.OrderedContent, error)
	after   func(message.Msg, pipeline.IProcessorForExecutor, *message.OrderedContent, *message.OrderedContent)
}

func NewOperator(
	initialState struct{},
	af func(message.Msg, *struct{}) (*message.OrderedContent, *message.OrderedContent, error),
	after func(message.Msg, pipeline.IProcessorForExecutor, *message.OrderedContent, *message.OrderedContent),
) pipeline.Executor {
	return &Operator{
		state:   &initialState,
		aggFunc: af,
		after:   after,
	}
}

func (af *Operator) Execute(m message.Msg, proc pipeline.IProcessorForExecutor) bool {
	content, pContent, err := af.aggFunc(m, af.state)

	if af.after == nil {
		if err != nil {
			log.Printf("[ERROR] %v", err)
			return false
		}

		proc.Result(m, content, pContent)
	} else {
		af.after(m, proc, content, pContent)
	}
	return true
}

func (*Operator) ExecutorType() pipeline.ExecutorType {
	return pipeline.TRANSFORM
}

func (*Operator) HasLocalState() bool {
	return true
}

func (af *Operator) SetName(name string) {
	af.name = name
}

func (af *Operator) Name() string {
	return af.name
}
