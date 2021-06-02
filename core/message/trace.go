package message

import "fmt"

type tracePath struct {
	StageId     uint32
	ProcessorId uint32
	MessageId   uint64
}

func (tp *tracePath) String() string {
	return fmt.Sprintf("{Id:%d, Stg:%d, Prc:%d}", tp.StageId, tp.ProcessorId, tp.MessageId)
}

type trace struct {
	enabled bool
	path    []tracePath
}

func newTraceRoot(enabled bool) trace {
	return trace{enabled: enabled}
}

func newTrace(fromMsg Msg) trace {
	if fromMsg.trace.enabled {
		return fromMsg.trace
	}

	t := trace{enabled: true}
	t.path = append(fromMsg.trace.path, tracePath{
		StageId:     fromMsg.stageId,
		ProcessorId: fromMsg.processorId,
		MessageId:   fromMsg.id,
	})

	return t
}

func (t *trace) Enabled() bool {
	return t.enabled
}

func (t *trace) String() string {
	if !t.enabled {
		return ""
	}

	return fmt.Sprintf("Trace%v", t.path)
}
