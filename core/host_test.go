package core

import (
	"github.com/n-is/canal/core/message"
	"github.com/n-is/canal/core/pipeline"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestHost(t *testing.T) {

	hostId := uint(1)

	var host *Host
	var n1, n2 *pipeline.Pipeline

	t.Run("Testing Host Creation", func(t *testing.T) {
		host = NewHost(hostId)

		assert.NotNil(t, host, "Host should have been created")
		assert.Equal(t, hostId, host.Id, "id should be equal")
		assert.Zero(t, len(host.Pipelines), "No networks have been added")
	})

	t.Run("Testing AddPipeline", func(t *testing.T) {
		network1Id := uint(1)
		network1 := host.AddPipeline(network1Id)

		assert.Equal(t, 1, len(host.Pipelines), "One network have been created")

		if n := host.Pipelines[network1Id]; !reflect.DeepEqual(network1, n) {
			t.Errorf("pipeline: got = %#v, want = %#v", n, network1)
		}

		network2Id := uint(2)
		network2 := host.AddPipeline(network2Id)

		assert.Equal(t, 2, len(host.Pipelines), "Two networks have been created")

		if n := host.Pipelines[network2Id]; !reflect.DeepEqual(network2, n) {
			t.Errorf("pipeline: got = %#v, want = %#v", n, network2)
		}

		n1 = network1
		n2 = network2
	})

	n1NumMsgs := int64(5)
	n2NumMsgs := int64(50)

	var n1Msg, n2Msg *message.Msg

	var n1Sink, n2Sink pipeline.Executor

	t.Run("Testing pipeline Filler", func(t *testing.T) {

		mf := message.NewFactory(n1.Id(), uint32(2), uint32(1))
		n1Msg = mf.NewExecute(nil, &message.MsgContent{"Greet": message.NewFieldValue("Nihao", message.STRING)})

		n1Sink = fillDummyNetwork(n1, n1NumMsgs)
		// startVerifyNetwork(t, n1, n1NumMsgs, n1Msg, n1Sink)

		mf = message.NewFactory(n2.Id(), uint32(2), uint32(1))
		n2Msg = mf.NewExecute(nil, &message.MsgContent{"Greet": message.NewFieldValue("Nihao", message.STRING)})

		n2Sink = fillDummyNetwork(n2, n2NumMsgs)
		// startVerifyNetwork(t, n2, n2NumMsgs, n2Msg, n2Sink)
	})
	// Pipelines have been filled

	t.Run("Testing loop", func(t *testing.T) {
		host.Start()

		checkSink(t, n1NumMsgs, n1Msg, n1Sink)
		checkSink(t, n2NumMsgs, n2Msg, n2Sink)
	})

	_ = n1
	_ = n2
}

func checkSink(t *testing.T, numMsgs int64, msg *message.Msg, sinkExec pipeline.Executor) {
	for i := int64(0); i < numMsgs; i++ {
		// Correct message Ids
		//msg.id = uint64(2 * (i + 1))
		//if s, ok := sinkExec.(*dummySink); ok {
		//	m := <-s.sunk
		//	if !reflect.DeepEqual(m, msg) {
		//		t.Errorf("Msg: got = %#v, want = %#v", m, msg)
		//	}
		//}
	}
}

//! Can't re-run the network
