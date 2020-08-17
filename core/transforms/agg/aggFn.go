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

func (af *Operator) Execute(m message.Msg, proc pipeline.IProcessorForExecutor) bool {
	contents, pContents, err := af.aggFunc(m, af.state)

	if af.after == nil {
		if err != nil {
			log.Printf("[ERROR] %v", err)
			return false
		}

		for i := range contents {
			if !(contents[i] == nil && pContents[i] == nil) {
				proc.Result(m, contents[i], pContents[i])
			}
		}
	} else {
		af.after(m, proc, contents, pContents)
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
