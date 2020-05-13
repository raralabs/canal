package pipeline

import (
	"github.com/raralabs/canal/core/message"
	"testing"
)

type dummyExecutor struct {
	exeType ExecutorType
}

func newDummyExecutor(typ ExecutorType) Executor {
	return &dummyExecutor{exeType: typ}
}

func (dummy *dummyExecutor) Execute(m message.Msg, proc *Processor) bool {
	proc.Result(m, m.Content())
	//proc.Close()
	return true
}

func (dummy *dummyExecutor) HasLocalState() bool {
	return false
}

func (dummy *dummyExecutor) ExecutorType() ExecutorType {
	return dummy.exeType
}

func (dummy *dummyExecutor) SetName(string) {

}

func (dummy *dummyExecutor) Name() string {
	return "DummySource"
}

func TestDummyExecutor(t *testing.T) {
	pipelineId := uint32(1)
	msgF := message.NewFactory(pipelineId, 1, 1)

	content := message.MsgContent{
		"value": message.NewFieldValue(12, message.INT)	,
	}

	msg := msgF.NewExecuteRoot(content, false)

	exec := newDummyExecutor(TRANSFORM)

	_ = msg
	_ = exec
	//_ = proc
}