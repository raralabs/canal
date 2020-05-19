package base_transforms

import (
	"github.com/raralabs/canal/core/message"
	"github.com/raralabs/canal/core/pipeline"
)

type DoOperator struct {
	name   string
	doFunc func(message.Msg, pipeline.IProcessorForExecutor) bool
}

func NewDoOperator(df func(message.Msg, pipeline.IProcessorForExecutor) bool) pipeline.Executor {
	return &DoOperator{doFunc: df}
}

func (df *DoOperator) Execute(m message.Msg, proc pipeline.IProcessorForExecutor) bool {
	return df.doFunc(m, proc)
}

func (df *DoOperator) ExecutorType() pipeline.ExecutorType {
	return pipeline.TRANSFORM
}

func (df *DoOperator) HasLocalState() bool {
	return false
}

func (df *DoOperator) SetName(name string) {
	df.name = name
}

func (df *DoOperator) Name() string {
	return df.name
}
