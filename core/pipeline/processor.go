package pipeline

import (
	"github.com/raralabs/canal/core/message"
	"sync"
)

// A Processor represents an entity that wraps up a executor and handles
// things like providing messages to the executor for execution, returning the
// result appropriately, and sending the result to the sendPool for further execution.
type Processor struct {
	id              uint32             // id of the transforms
	processorPool   *processorPool     //
	executor        Executor           // executor associated with the Processor, can have only one executor
	routes          msgRoutes          // routes for which the Processor should process the messages
	sendPool        sendPool           // The sendPool to fanout the messages produced by this Processor to all its listeners
	errorSender     chan<- message.Msg //
	mesFactory      message.Factory    // Msg Factory associated with the Processor. Helps in generating new messages.
	lastReceivedMid uint64             // id of the last received msg by the Processor
	totalReceived   uint64             //
	processLock     sync.Mutex         //
}

func (pr *Processor) Id() uint32 {
	return pr.id
}

func (pr *Processor) lock(stgRoutes msgRoutes) {
	if !pr.isConnected() {
		panic("Pipeline not configured correctly.")
	}

	for route, _ := range pr.routes {
		if _, ok := stgRoutes[route]; !ok {
			panic("Subscribing messages from a non existing route.")
		}
	}

	if pr.executor.ExecutorType() != SINK {
		pr.sendPool.lock()
	}
}

// process executes the corresponding executor of the process on the passed
// msg. Returns true on successful execution of the executor on msg.
func (pr *Processor) process(pod msgPod) bool {
	if _, ok := pr.routes[pod.routeName]; len(pr.routes) > 0 && !ok {
		return false
	}

	pr.processLock.Lock()
	pr.lastReceivedMid = pod.msg.Id()
	pr.totalReceived++
	pr.processLock.Unlock()

	return pr.executor.Execute(pod.msg, pr)
}

func (pr *Processor) Result(msg message.Msg, content message.MsgContent) {
	if pr.isClosed() || pr.executor.ExecutorType() == SINK {
		return
	}

	m := pr.mesFactory.NewExecute(msg, content)

	//Send the messages one by one
	pr.processLock.Lock()
	// If sendPool can't send the messages, then there's no point in processing, so Close
	if !pr.sendPool.send(m, false) {
		println("Closing processor, could not send ", pr.sendPool.processor.processorPool.stage.name)
		pr.Close()
	}
	pr.processLock.Unlock()
}

func (pr *Processor) Error(code uint8, err error) {
	pr.errorSender <- pr.mesFactory.NewError(nil, code, err.Error())
}

// Close closes the Processor. It essentially closes the sendPool if ExecutorType is not SINK.
func (pr *Processor) Close() {
	if pr.executor.ExecutorType() != SINK {
		pr.sendPool.close()
	}
}

func (pr *Processor) isClosed() bool {
	if pr.executor.ExecutorType() == SINK {
		return false
	}

	return pr.sendPool.isClosed()
}

// addSendTo connects a Processor to to other Stage's receivePool to send messages there.
func (pr *Processor) addSendTo(stage *stage, route string) {
	pr.sendPool.addSendTo(stage, route)
}

func (pr *Processor) channelForStageId(stage *stage) <-chan msgPod {
	return pr.sendPool.getChannel(stage)
}

func (pr *Processor) isConnected() bool {
	if pr.executor.ExecutorType() == SINK {
		return true
	}
	return pr.sendPool.isConnected()
}

// statusMessage creates a new msg with certain variables of interest.
func (pr *Processor) statusMessage(withTrace bool) message.Msg {
	status := make(message.MsgContent)
	status.AddMessageValue("lastSentMid", message.NewFieldValue(pr.sendPool.lastSentMid, message.INT))
	status.AddMessageValue("lastReceivedMid", message.NewFieldValue(pr.lastReceivedMid, message.INT))

	mes := pr.mesFactory.NewExecuteRoot(status, withTrace)

	return mes
}
