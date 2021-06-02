package agg

import (
	"github.com/raralabs/canal/core/message/content"
	"log"

	"github.com/raralabs/canal/core/message"
	"github.com/raralabs/canal/core/pipeline"
)

type Operator struct {
	name    string
	state   *struct{}
	aggFunc func(message.Msg, *struct{}) ([]content.IContent, []content.IContent, error)
	after   func(message.Msg, pipeline.IProcessorForExecutor, []content.IContent, []content.IContent)
}

func NewOperator(
	initialState struct{},
	af func(message.Msg, *struct{}) ([]content.IContent, []content.IContent, error),
	after func(message.Msg, pipeline.IProcessorForExecutor, []content.IContent, []content.IContent),
) pipeline.Executor {
	return &Operator{
		state:   &initialState,
		aggFunc: af,
		after:   after,
	}
}

func (af *Operator) Execute(m pipeline.MsgPod, proc pipeline.IProcessorForExecutor) bool {
	contents, pContents, err := af.aggFunc(m.Msg, af.state)
	if err != nil {
		log.Printf("[WARN] %v", err)
	}

	if af.after == nil {
		if m.Msg.Eof() {
			proc.Result(m.Msg, nil, nil)
			proc.Done()
		} else if len(contents) > 0 {
			i := len(contents) - 1
			if !(contents[i] == nil && pContents[i] == nil) {
				proc.Result(m.Msg, contents[i], pContents[i])
			}
		}
	} else {
		af.after(m.Msg, proc, contents, pContents)
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
