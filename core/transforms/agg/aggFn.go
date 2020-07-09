package agg

import (
	"github.com/raralabs/canal/core/message"
	"github.com/raralabs/canal/core/pipeline"
)

type Operator struct {
	name    string
	state   *struct{}
	toMsg   func(*struct{}) []*message.OrderedContent
	aggFunc func(message.Msg, *struct{}) (bool, error)
	after   func(message.Msg, pipeline.IProcessorForExecutor, []*message.OrderedContent) bool
}

func NewOperator(
	initialState struct{},
	tmf func(*struct{}) []*message.OrderedContent,
	af func(message.Msg, *struct{}) (bool, error),
	after func(message.Msg, pipeline.IProcessorForExecutor, []*message.OrderedContent) bool,
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

	// Pass all the messages ahead if there is no after handler
	if af.after == nil {
		for _, msg := range msgs {
			proc.Result(m, msg)
		}
	} else { // Handle all the messages in after handler
		af.after(m, proc, msgs)
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
