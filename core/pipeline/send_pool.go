package pipeline

import (
	"github.com/raralabs/canal/core/message"
	"sync/atomic"
	"time"
)

const (
	_SendBufferLength uint16 = 2
	_SendTimeout             = 1 * time.Second
)

// A sendPool does the Fanout of messages to all connected receivers
type sendPool struct {
	pipeline    *Pipeline            //
	stageId     uint32               //
	processorId uint32               //
	sendRoutes  map[uint32]sendRoute //
	errorSender chan<- message.Msg   //
	closed      atomic.Value         //
	runLock     atomic.Value         //
	lastSentMid uint64               // id of the last sent msg by the Processor
	totalSent   uint64               //
}

// newSendPool creates a new receivePool
func newSendPool(pipeline *Pipeline, stageId uint32, processorId uint32) sendPool {
	return sendPool{
		pipeline:    pipeline,
		stageId:     stageId,
		processorId: processorId,
		sendRoutes:  make(map[uint32]sendRoute),
		errorSender: pipeline.errorReceiver,
	}
}

func (sp *sendPool) isConnected() bool {
	return len(sp.sendRoutes) > 0
}

// addSendTo registers a stage to which the sendPool is supposed to send the msg.
func (sp *sendPool) addSendTo(stage *stage, route string) {
	if sp.isLocked() {
		return
	}

	if _, ok := sp.sendRoutes[stage.id]; !ok {
		sp.sendRoutes[stage.id] = newSendRoute(make(chan msgPod, _SendBufferLength), route)
	}
}

func (sp *sendPool) getChannel(stageId uint32) <-chan msgPod {
	readPath, ok := sp.sendRoutes[stageId]

	if !ok {
		panic("Trying to get ReadChannel for stage not connected")
	}

	return readPath.sendChannel
}

// send queues the messages to be sent to all the routes in the sendPool.
func (sp *sendPool) send(mes message.Msg, dropOnTimeout bool) bool {
	if sp.isClosed() || !sp.isLocked() {
		return false
	}

	sent := false
	for stageId, route := range sp.sendRoutes {
		if sp.pipeline.GetStage(stageId).isClosed() {
			continue
		}

		sent = sent || route.send(mes, _SendTimeout, func() bool {
			sp.error(1, "Timeout in sending "+route.routeName)
			return dropOnTimeout
		})
	}

	sp.lastSentMid = mes.Id()
	sp.totalSent++

	return sent
}

func (sp *sendPool) error(code uint8, text string) {
	sp.errorSender <- message.NewError(sp.pipeline.id, sp.stageId, sp.processorId, code, text)
}

func (sp *sendPool) isClosed() bool {
	c := sp.closed.Load()
	return c != nil && c.(bool)
}

func (sp *sendPool) isLocked() bool {
	c := sp.runLock.Load()
	return c != nil && c.(bool)
}

func (sp *sendPool) lock() {
	if sp.isLocked() {
		return
	}
	sp.runLock.Store(true)
}

// Close closes the sendPool
func (sp *sendPool) close() {
	if sp.isClosed() {
		return
	}

	sp.closed.Store(true)
	for _, sendRoute := range sp.sendRoutes {
		close(sendRoute.sendChannel)
	}
}
