package base_transforms

import (
	"github.com/raralabs/canal/core/message"
	"github.com/raralabs/canal/core/pipeline"
)

type AggOperator struct {
	name    string
	state   *struct{}
	toMsg   func(*struct{}) []*message.MsgContent
	aggFunc func(message.Msg, *struct{}) (bool, error)
}

func NewAggOperator(
	initialState struct{},
	tmf func(*struct{}) []*message.MsgContent,
	af func(message.Msg, *struct{}) (bool, error),
) pipeline.Executor {
	return &AggOperator{
		state:   &initialState,
		toMsg:   tmf,
		aggFunc: af,
	}
}

func (af *AggOperator) Execute(m message.Msg, proc pipeline.IProcessorForExecutor) bool {
	done, _ := af.aggFunc(m, af.state)
	return done
}

func (*AggOperator) ExecutorType() pipeline.ExecutorType {
	return pipeline.TRANSFORM
}

func (*AggOperator) HasLocalState() bool {
	return true
}

func (af *AggOperator) SetName(name string) {
	af.name = name
}

func (af *AggOperator) Name() string {
	return af.name
}
