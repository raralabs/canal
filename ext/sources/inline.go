package sources

import (
	"github.com/raralabs/canal/core/message"
	content2 "github.com/raralabs/canal/core/message/content"
	"github.com/raralabs/canal/core/pipeline"
)

type InlineRange struct {
	name   string
	curVal uint64
	maxVal uint64
}

func NewInlineRange(maxVal uint64) pipeline.Executor {
	return &InlineRange{name: "Inline", curVal: 0, maxVal: maxVal}
}

func (s *InlineRange) Execute(m message.Msg, proc pipeline.IProcessorForExecutor) bool {
	if s.curVal >= s.maxVal {
		proc.Done()
		return true
	}

	s.curVal++
	content := content2.New()
	content.Add("value", content2.NewFieldValue(s.curVal, content2.INT))
	proc.Result(m, content, nil)
	return false
}

func (s *InlineRange) ExecutorType() pipeline.ExecutorType {
	return pipeline.SOURCE
}

func (s *InlineRange) HasLocalState() bool {
	return false
}

func (s *InlineRange) SetName(name string) {
	s.name = name
}

func (s *InlineRange) Name() string {
	return s.name
}
