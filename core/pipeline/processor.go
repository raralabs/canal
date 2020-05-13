package pipeline

import (
	"github.com/raralabs/canal/core/message"
	"sync"
)

// A Processor represents an entity that wraps up a executor and handles
// things like providing messages to the executor for execution, returning the
// result appropriately, and sending the result to the sndPool for further execution.
type Processor struct {
	id         uint32             // id of the transforms
	procPool   *processorPool     //
	executor   Executor           // executor associated with the Processor, can have only one executor
	routes     msgRoutes          // routes for which the Processor should process the messages
	sndPool    sendPool           // The sndPool to fanout the messages produced by this Processor to all its listeners
	errSender  chan<- message.Msg //
	mesFactory message.Factory    // Msg Factory associated with the Processor. Helps in generating new messages.
	lastRcvMid uint64             // id of the last received msg by the Processor
	totalRcv   uint64             //
	procLock   sync.Mutex         //
}

func (pr *Processor) lock(stgRoutes msgRoutes) {
	if !pr.isConnected() {
		panic("Pipeline not configured correctly.")
	}

	for route := range pr.routes {
		if _, ok := stgRoutes[route]; !ok {
			panic("Subscribing messages from a non existing route.")
		}
	}

	if pr.executor.ExecutorType() != SINK {
		pr.sndPool.lock()
	}
}

// process executes the corresponding executor of the process on the passed
// msg. Returns true on successful execution of the executor on msg.
func (pr *Processor) process(pod msgPod) bool {
	if _, ok := pr.routes[pod.routeName]; len(pr.routes) > 0 && !ok {
		return false
	}

	pr.procLock.Lock()
	pr.lastRcvMid = pod.msg.Id()
	pr.totalRcv++
	pr.procLock.Unlock()

	return pr.executor.Execute(pod.msg, pr)
}

func (pr *Processor) Result(srcMsg message.Msg, content message.MsgContent) {
	if pr.isClosed() || pr.executor.ExecutorType() == SINK {
		return
	}

	m := pr.mesFactory.NewExecute(srcMsg, content)

	//Send the messages one by one
	pr.procLock.Lock()
	// If sndPool can't send the messages, then there's no point in processing, so Close
	if !pr.sndPool.send(m, false) {
		println("Closing proc, could not send ", pr.sndPool.proc.procPool.stage.name)
		pr.Close()
	}
	pr.procLock.Unlock()
}

func (pr *Processor) Error(code uint8, err error) {
	pr.errSender <- pr.mesFactory.NewError(nil, code, err.Error())
}

// Close closes the Processor. It essentially closes the sndPool if ExecutorType is not SINK.
func (pr *Processor) Close() {
	if pr.executor.ExecutorType() != SINK {
		pr.sndPool.close()
	}
}

func (pr *Processor) isClosed() bool {
	if pr.executor.ExecutorType() == SINK {
		return false
	}

	return pr.sndPool.isClosed()
}

// addSendTo connects a Processor to to other Stage's receivePool to send messages there.
func (pr *Processor) addSendTo(stage *stage, route string) {
	pr.sndPool.addSendTo(stage, route)
}

func (pr *Processor) channelForStageId(stage *stage) <-chan msgPod {
	return pr.sndPool.getChannel(stage)
}

func (pr *Processor) isConnected() bool {
	if pr.executor.ExecutorType() == SINK {
		return true
	}
	return pr.sndPool.isConnected()
}

// statusMessage creates a new msg with certain variables of interest.
func (pr *Processor) statusMessage(withTrace bool) message.Msg {
	status := make(message.MsgContent)
	status.AddMessageValue("lastSndMid", message.NewFieldValue(pr.sndPool.lastSndMid, message.INT))
	status.AddMessageValue("lastRcvMid", message.NewFieldValue(pr.lastRcvMid, message.INT))

	mes := pr.mesFactory.NewExecuteRoot(status, withTrace)

	return mes
}
