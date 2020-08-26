package message

import (
	"github.com/raralabs/canal/core/message/content"
	"sync/atomic"
)

// A MessageFactory represents a factory that can produce message(s).
type Factory struct {
	pipelineId  uint32 //
	stageId     uint32 //
	processorId uint32 //
	HWM         uint64 // hwm is used to provide id to the message.
}

// newMessageFactory creates a new un-traceable message producing factory on the
// basis of provided parameters.
func NewFactory(pipelineId uint32, stageId uint32, processorId uint32) Factory {
	return Factory{pipelineId: pipelineId, stageId: stageId, processorId: processorId, HWM: 0}
}

// NewExecute creates a new message with the 'value' as actual data and returns it.
func (mf *Factory) NewExecuteRoot(content content.IContent, withTrace bool) Msg {
	traceRoot := newTraceRoot(withTrace)
	return Msg{
		id:          atomic.AddUint64(&mf.HWM, 1),
		pipelineId:  mf.pipelineId,
		stageId:     mf.stageId,
		processorId: mf.processorId,
		mtype:       EXECUTE,
		mcontent:    content,
		prevContent: nil,
		trace:       traceRoot,
	}
}

// NewExecute creates a new message with the 'value' as actual data and returns it.
func (mf *Factory) NewExecute(srcMessage Msg, contents content.IContent, pContent content.IContent) Msg {
	m := Msg{
		id:             atomic.AddUint64(&mf.HWM, 1),
		pipelineId:     mf.pipelineId,
		stageId:        mf.stageId,
		processorId:    mf.processorId,
		srcStageId:     srcMessage.stageId,
		srcProcessorId: srcMessage.processorId,
		srcMessageId:   srcMessage.id,
		mtype:          EXECUTE,
		trace:          newTrace(srcMessage),
	}
	if contents != nil {
		m.mcontent = contents.Copy()
	}
	if pContent != nil {
		m.prevContent = pContent.Copy()
	}

	return m
}

// NewError creates a new message with the 'value' as actual data and returns it.
func (mf *Factory) NewError(srcMessage *Msg, code uint8, mes string) Msg {
	var ssId, spId uint32
	var smId uint64
	if srcMessage != nil {
		ssId, spId, smId = srcMessage.stageId, srcMessage.processorId, srcMessage.id
	}

	contents := content.New()
	contents.Add("text", content.NewFieldValue(mes, content.STRING))
	contents.Add("code", content.NewFieldValue(code, content.INT))

	return Msg{
		id:             atomic.AddUint64(&mf.HWM, 1),
		pipelineId:     mf.pipelineId,
		stageId:        mf.stageId,
		processorId:    mf.processorId,
		srcStageId:     ssId,
		srcProcessorId: spId,
		srcMessageId:   smId,
		mtype:          ERROR,
		mcontent:       contents,
	}
}
