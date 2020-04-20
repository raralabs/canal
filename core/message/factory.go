package message

import "sync/atomic"

// A MessageFactory represents a factory that can produce message(s).
type Factory struct {
	PipelineId  uint32 //
	StageId     uint32 //
	ProcessorId uint32 //
	HWM         uint64 // hwm is used to provide id to the message.
}

// newMessageFactory creates a new un-traceable message producing factory on the
// basis of provided parameters.
func NewFactory(pipelineId uint32, stageId uint32, processorId uint32) Factory {
	return Factory{PipelineId: pipelineId, StageId: stageId, ProcessorId: processorId, HWM: 0}
}

// NewExecute creates a new message with the 'value' as actual data and returns it.
func (mf *Factory) NewExecuteRoot(content MsgContent, withTrace bool) Msg {
	traceRoot := newTraceRoot(withTrace)
	return Msg{
		id:          atomic.AddUint64(&mf.HWM, 1),
		pipelineId:  mf.PipelineId,
		stageId:     mf.StageId,
		processorId: mf.ProcessorId,
		mtype:       EXECUTE,
		mcontent:    content,
		trace:       traceRoot,
	}
}

// NewExecute creates a new message with the 'value' as actual data and returns it.
func (mf *Factory) NewExecute(srcMessage Msg, content MsgContent) Msg {
	return Msg{
		id:             atomic.AddUint64(&mf.HWM, 1),
		pipelineId:     mf.PipelineId,
		stageId:        mf.StageId,
		processorId:    mf.ProcessorId,
		srcStageId:     srcMessage.stageId,
		srcProcessorId: srcMessage.processorId,
		srcMessageId:   srcMessage.id,
		mtype:          EXECUTE,
		mcontent:       content,
		trace:          newTrace(srcMessage),
	}
}

// NewError creates a new message with the 'value' as actual data and returns it.
func (mf *Factory) NewError(srcMessage *Msg, code uint8, mes string) Msg {
	var ssId, spId uint32
	var smId uint64
	if srcMessage != nil {
		ssId, spId, smId = srcMessage.stageId, srcMessage.processorId, srcMessage.id
	}

	content := make(MsgContent)
	content.AddMessageValue("text", NewFieldValue(mes, STRING))
	content.AddMessageValue("code", NewFieldValue(code, INT))

	return Msg{
		id:             atomic.AddUint64(&mf.HWM, 1),
		pipelineId:     mf.PipelineId,
		stageId:        mf.StageId,
		processorId:    mf.ProcessorId,
		srcStageId:     ssId,
		srcProcessorId: spId,
		srcMessageId:   smId,
		mtype:          ERROR,
		mcontent:       content,
	}
}
