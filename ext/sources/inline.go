package sources

import (
	"github.com/raralabs/canal/core/message"
	"github.com/raralabs/canal/core/message/content"
	"github.com/raralabs/canal/core/pipeline"
)

type InlineRange struct {
	name   string
	curVal int64
	maxVal int64
}

func NewInlineRange(maxVal int64) pipeline.Executor {
	return &InlineRange{name: "Inline", curVal: 0, maxVal: maxVal}
}

func (s *InlineRange) Execute(m pipeline.MsgPod, proc pipeline.IProcessorForExecutor) bool {
	if s.curVal >= s.maxVal {
		msg := message.NewEof(m.Msg.PipelineId(), m.Msg.StageId(), m.Msg.ProcessorId())
		proc.Result(msg, nil, nil)
		proc.Done()
		return true
	}

	s.curVal++
	msgContent := content.New()
	msgContent = msgContent.Add("value", content.NewFieldValue(s.curVal, content.INT))
	proc.Result(m.Msg, msgContent, nil)
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
