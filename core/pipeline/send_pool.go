package pipeline

import (
	"sync/atomic"
	"time"

	"github.com/raralabs/canal/core/message"
)

const (
	_SendBufferLength uint16 = 4
	_SendTimeout             = 1 * time.Second
)
// A sndPool does the Fanout of messages to all connected receivers
type sendPool struct {
	proc       *Processor           //
	sndRoutes  map[*stage]sendRoute //
	errSender  chan<- message.Msg   //
	lastSndMid uint64               // id of the last sent msg by the Processor
	totalSnd   uint64               //
	closed     atomic.Value         //
	runLock    atomic.Value         //
}

// newSendPool creates a new send Pool
func newSendPool(processor *Processor) sendPool {
	return sendPool{
		proc:      processor,
		sndRoutes: make(map[*stage]sendRoute),
		errSender: processor.errSender,
	}
}

func (sp *sendPool) isConnected() bool {
	return len(sp.sndRoutes) > 0
}

// addSendTo registers a stg to which the sndPool is supposed to send the msg.
func (sp *sendPool) addSendTo(stg *stage, route MsgRouteParam) {
	if sp.isLocked() {
		return
	}

	if _, ok := sp.sndRoutes[stg]; !ok {
		sp.sndRoutes[stg] = newSendRoute(make(chan msgPod, _SendBufferLength), route)
	}
}

func (sp *sendPool) getChannel(stg *stage) <-chan msgPod {
	readPath, ok := sp.sndRoutes[stg]

	if !ok {
		panic("Trying to get ReadChannel for stg not connected")
	}

	return readPath.sendChannel
}

// send queues the messages to be sent to all the routes in the sndPool.
func (sp *sendPool) send(mes message.Msg, dropOnTimeout bool) bool {
	if sp.isClosed() || !sp.isLocked() {
		return false
	}

	sent := false
	for stg, route := range sp.sndRoutes {
		if stg.isClosed() {
			continue
		}

		onTimeout := func() bool {
			sp.error(1, "Timeout in sending to "+string(route.route))
			return dropOnTimeout
		}
		snt := route.send(mes, _SendTimeout, onTimeout)
		sent = sent || snt
	}

	if sent {
		sp.lastSndMid = mes.Id()
		sp.totalSnd++
	}

	return sent
}

func (sp *sendPool) error(code uint8, text string) {
	sp.errSender <- message.NewError(
		sp.proc.procPool.stage().pipeline.id,
		sp.proc.procPool.stage().id,
		sp.proc.id,
		code, text)
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

// Done closes the sndPool
func (sp *sendPool) close() {
	if sp.isClosed() {
		return
	}

	sp.closed.Store(true)
	for _, sendRoute := range sp.sndRoutes {
		close(sendRoute.sendChannel)
	}
}
