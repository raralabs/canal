package sinks

import (

	"github.com/raralabs/canal/core/pipeline"
)

type BlackholeSink struct {
	name string
}

func NewBlackholeSink() pipeline.Executor {
	return &BlackholeSink{name: "BlackHole"}
}

func (s *BlackholeSink) ExecutorType() pipeline.ExecutorType {
	return pipeline.SINK
}

func (s *BlackholeSink) Execute(_ pipeline.MsgPod, _ pipeline.IProcessorForExecutor) bool {
	return true
}


func (s *BlackholeSink) HasLocalState() bool {
	return false
}

func (s *BlackholeSink) SetName(name string) {
	s.name = name
}

func (s *BlackholeSink) Name() string {
	return s.name
}
