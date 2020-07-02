package do

import (
	"github.com/raralabs/canal/core/message"
	"github.com/raralabs/canal/core/pipeline"
)

type Operator struct {
	name   string
	doFunc func(message.Msg, pipeline.IProcessorForExecutor) bool
}

func NewOperator(df func(message.Msg, pipeline.IProcessorForExecutor) bool) pipeline.Executor {
	return &Operator{doFunc: df}
}

func (df *Operator) Execute(m message.Msg, proc pipeline.IProcessorForExecutor) bool {
	return df.doFunc(m, proc)
}

func (df *Operator) ExecutorType() pipeline.ExecutorType {
	return pipeline.TRANSFORM
}

func (df *Operator) HasLocalState() bool {
	return false
}

func (df *Operator) SetName(name string) {
	df.name = name
}

func (df *Operator) Name() string {
	return df.name
}
