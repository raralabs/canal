package tfm

import (
	"github.com/n-is/canal/core/message"
	"github.com/n-is/canal/core/pipeline"
)

type AggTfm struct {
	name    string
	state   *struct{}
	trigger func(*struct{}) bool
	toMsg   func(*message.Msg, *struct{}) []*message.MsgContent
	aggFunc func(*message.Msg, *struct{}) (bool, error)
}

func NewAggTfm(
	initialState struct{},
	tf func(*struct{}) bool,
	tmf func(*message.Msg, *struct{}) []*message.MsgContent,
	af func(*message.Msg, *struct{}) (bool, error),
) pipeline.Executor {
	return &AggTfm{
		state:   &initialState,
		trigger: tf,
		toMsg:   tmf,
		aggFunc: af,
	}
}

func (af *AggTfm) Execute(m message.Msg) ([]message.MsgContent, bool, error) {
	return []*message.MsgContent{}, true, nil
}

func (*AggTfm) ExecutorType() pipeline.ExecutorType {
	return pipeline.TRANSFORM
}

func (*AggTfm) HasLocalState() bool {
	return true
}

func (af *AggTfm) SetName(name string) {
	af.name = name
}

func (af *AggTfm) Name() string {
	return af.name
}
