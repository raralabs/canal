package sinks

import (
	"fmt"
	"github.com/raralabs/canal/core/message"
	"github.com/raralabs/canal/core/pipeline"
)

type StdoutSink struct {
	name string
}

func NewStdoutSink() pipeline.Executor {
	return &StdoutSink{name: "StdOut"}
}

func (s *StdoutSink) ExecutorType() pipeline.ExecutorType {
	return pipeline.SINK
}

func (s *StdoutSink) Execute(m message.Msg, _ *pipeline.Processor) bool {
	var trace string
	if m.Trace() != nil {
		trace = m.Trace().String()
	}

	fmt.Println(fmt.Sprintf("[StdoutSink] %s %s", m.String(), trace))

	return false
}

func (s *StdoutSink) HasLocalState() bool {
	return false
}

func (s *StdoutSink) SetName(name string) {
	s.name = name
}

func (s *StdoutSink) Name() string {
	return s.name
}
