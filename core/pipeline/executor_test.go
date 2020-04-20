package pipeline

import (
	"github.com/raralabs/canal/core/message"
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

func (dummy *dummyExecutor) SetName(name string) {

}

func (dummy *dummyExecutor) Name() string {
	return "DummySource"
}
