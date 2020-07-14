package agg

import (
	"github.com/raralabs/canal/core/message"
	"github.com/raralabs/canal/core/pipeline"
	"log"
)

type Operator struct {
	name    string
	state   *struct{}
	aggFunc func(message.Msg, *struct{}) (*message.OrderedContent, error)
	after   func(message.Msg, pipeline.IProcessorForExecutor, *message.OrderedContent)
}

func NewOperator(
	initialState struct{},
	af func(message.Msg, *struct{}) (*message.OrderedContent, error),
	after func(message.Msg, pipeline.IProcessorForExecutor, *message.OrderedContent),
) pipeline.Executor {
	return &Operator{
		state:   &initialState,
		aggFunc: af,
		after:   after,
	}
}

func (af *Operator) Execute(m message.Msg, proc pipeline.IProcessorForExecutor) bool {
	msg, err := af.aggFunc(m, af.state)

	if err != nil {
		log.Printf("[ERROR] %v", err)
		return false
	}

	if af.after == nil {
		proc.Result(m, msg)
	} else {
		af.after(m, proc, msg)
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
