package pipeline

import (
	content2 "github.com/raralabs/canal/core/message/content"
	"reflect"
	"testing"

	"github.com/raralabs/canal/core/message"
	"github.com/stretchr/testify/assert"
)

type dummyExecutor struct {
	exeType ExecutorType
}

func newDummyExecutor(typ ExecutorType) Executor {
	return &dummyExecutor{exeType: typ}
}
func (dummy *dummyExecutor) Execute(mpd MsgPod, proc IProcessorForExecutor) bool {
	m := mpd.Msg
	proc.Result(m, m.Content(), nil)
	//proc.Done()
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

	content := content2.New()
	content = content.Add("value", content2.NewFieldValue(12, content2.INT))

	msg := msgF.NewExecuteRoot(content, false)

	exec := newDummyExecutor(TRANSFORM)
	proc := newDummyProcessorExecutor(exec)

	proc.process(msg)
	if !reflect.DeepEqual(msg, proc.resSrcMsg) {
		t.Errorf("want: %v, got: %v", proc.resSrcMsg, msg)
	}

	_ = msg
	_ = exec
	_ = proc
}

func TestExecutorType_String(t *testing.T) {
	et := SOURCE
	assert.Equal(t, "SOURCE", et.String())
	et = TRANSFORM
	assert.Equal(t, "TRANSFORM", et.String())
	et = SINK
	assert.Equal(t, "SINK", et.String())

	et = ExecutorType(255)
	assert.Equal(t, "UNKNOWN", et.String())
}
