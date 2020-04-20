package do

import (
	"github.com/n-is/canal/core/message"
	"github.com/n-is/canal/core/pipeline"
)

type DoFunc struct {
	name   string
	doFunc func(*message.Msg) ([]message.MsgContent, bool, error)
}

func NewDoFunc(name string, df func(*message.Msg) ([]message.MsgContent, bool, error)) pipeline.Executor {
	return &DoFunc{name: name, doFunc: df}
}

func (df *DoFunc) Execute(m message.Msg) ([]message.MsgContent, bool, error) {
	return df.doFunc(m)
}

func (df *DoFunc) ExecutorType() pipeline.ExecutorType {
	return pipeline.TRANSFORM
}

func (df *DoFunc) HasLocalState() bool {
	return false
}

func (df *DoFunc) SetName(name string) {
	df.name = name
}

func (df *DoFunc) Name() string {
	return df.name
}
