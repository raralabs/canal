package core

import (
	"context"
	"github.com/n-is/canal/core/message"
	"github.com/n-is/canal/core/pipeline"
	"testing"
)

// dummyExecutor acts as a mock for executor with ValueType = TRANSFORM
type dummyExecutor struct {
}

func newDummyExecutor() pipeline.Executor {
	return &dummyExecutor{}
}

func (d *dummyExecutor) Execute(m message.Msg) ([]message.MsgContent, bool, error) {
	content := m.Content()
	return []*message.MsgContent{&content}, true, nil
}

func (d *dummyExecutor) HasLocalState() bool {
	return false
}

func (d *dummyExecutor) ExecutorType() pipeline.ExecutorType {
	return pipeline.TRANSFORM
}

func (d *dummyExecutor) Name() string {
	return "DummyExecutor"
}

func (d *dummyExecutor) SetName(string) {}

//
// dummySource acts as a mock for executor with ValueType = SOURCE
type dummySource struct {
	nums, max int64
}

func newDummySource(max int64) pipeline.Executor {
	return &dummySource{max: max}
}

func (d *dummySource) Execute(m message.Msg) ([]message.MsgContent, bool, error) {
	msgMap := make(message.MsgContent)

	if d.nums >= d.max {
		msgMap = nil
	} else {
		msgMap := make(message.MsgContent)
		msgMap.AddMessageValue("Greet", message.NewFieldValue("Nihao", message.STRING))
	}

	d.nums++

	return []*message.MsgContent{&msgMap}, true, nil
}

func (d *dummySource) HasLocalState() bool {
	return false
}

func (d *dummySource) ExecutorType() pipeline.ExecutorType {
	return pipeline.SOURCE
}

func (d *dummySource) Name() string {
	return "DummySource"
}

func (d *dummySource) SetName(string) {}

//
// dummySink acts as a mock for executor with ValueType = SINK
type dummySink struct {
	sunk chan *message.Msg
}

func newDummySink() pipeline.Executor {
	return &dummySink{sunk: make(chan *message.Msg, 100)}
}

func (d *dummySink) Execute(m message.Msg) ([]message.MsgContent, bool, error) {
	d.sunk <- m
	return nil, true, nil
}

func (d *dummySink) HasLocalState() bool {
	return false
}

func (d *dummySink) ExecutorType() pipeline.ExecutorType {
	return pipeline.SINK
}

func (d *dummySink) Name() string {
	return "DummySink"
}

func (d *dummySink) SetName(string) {}

//
// dummyNetwork filler
func fillDummyNetwork(n *pipeline.Pipeline, numMsgs int64) pipeline.Executor {

	n.AddSource()

	// add source job(transforms) to the hub
	exec := newDummySource(numMsgs)
	srcJob := n.GetStages()[0].AddProcessor(exec)

	n.AddTransform()

	// add transforms job(transforms) to the hub
	exec = newDummyExecutor()
	prcJob := n.GetStages()[1].AddProcessor(exec)
	n.GetStages()[1].ReceiveFrom("default", srcJob)

	n.AddSink()

	// add transforms job(transforms) to the hub
	exec = newDummySink()
	n.GetStages()[2].AddProcessor(exec)

	n.GetStages()[2].ReceiveFrom("default", prcJob)

	// Return the sink executor
	return exec
}

func startVerifyNetwork(t *testing.T, n *pipeline.Pipeline, numMsgs int64, msg *message.Msg, snkExec pipeline.Executor) {
	t.Run("Testing pipeline", func(t *testing.T) {

		f := func() {

			for i := int64(0); i < numMsgs; i++ {
				// Correct message Ids
				//msg.id = uint64(2 * (i + 1))
				//if s, ok := snkExec.(*dummySink); ok {
				//	m := <-s.sunk
				//	if !reflect.DeepEqual(m, msg) {
				//		t.Errorf("Msg: got = %#v, want = %#v", m, msg)
				//	}
				//}
			}
		}

		n.Start(context.Background(), f)
	})
}
