package sinks

import (
	"fmt"

	//"github.com/raralabs/canal/core/message"
	"github.com/raralabs/canal/core/pipeline"
)

type StdoutSink struct {
	name   string
	header []string
}

func NewStdoutSink(header ...string) pipeline.Executor {
	return &StdoutSink{
		name:   "StdOut",
		header: header,
	}
}

func (s *StdoutSink) ExecutorType() pipeline.ExecutorType {
	return pipeline.SINK
}

func (s *StdoutSink) Execute(m pipeline.MsgPod, proc pipeline.IProcessorForExecutor) bool {

	if m.Msg.Eof() {
		proc.Done()
		return false
	}

	var trace string
	if m.Msg.Trace() != nil {
		trace = m.Msg.Trace().String()
	}
	contents := m.Msg.Content()

	fmt.Print("[StdoutSink] ")
	if s.header == nil || len(s.header) == 0 {
		fmt.Println(fmt.Sprintf("%s %s", m.Msg.String(), trace))

	} else {
		fmt.Printf("Msg[Id:%d, Stg:%d, Prc:%d; Contents:{", m.Msg.Id(), m.Msg.StageId(), m.Msg.ProcessorId())
		for _, k := range s.header {
			if v, ok := contents.Get(k); ok {
				fmt.Printf(" %s: %v ", k, v)
			}
		}
		fmt.Println("}]", trace)
	}

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
