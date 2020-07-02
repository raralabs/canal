package agg

import (
	"github.com/raralabs/canal/core/message"
	"github.com/raralabs/canal/core/pipeline"
)

type Operator struct {
	name    string
	state   *struct{}
	toMsg   func(*struct{}) []*message.MsgContent
	aggFunc func(message.Msg, *struct{}) (bool, error)
	after   func(message.Msg, pipeline.IProcessorForExecutor) bool
}

func NewOperator(
	initialState struct{},
	tmf func(*struct{}) []*message.MsgContent,
	af func(message.Msg, *struct{}) (bool, error),
	after func(message.Msg, pipeline.IProcessorForExecutor) bool,
) pipeline.Executor {
	return &Operator{
		state:   &initialState,
		toMsg:   tmf,
		aggFunc: af,
		after:   after,
	}
}

func (af *Operator) Execute(m message.Msg, proc pipeline.IProcessorForExecutor) bool {
	done, _ := af.aggFunc(m, af.state)
	msgs := af.toMsg(af.state)

	for _, msg := range msgs {
		proc.Result(m, *msg)
	}

	if af.after != nil {
		af.after(m, proc)
	}

	return done
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
