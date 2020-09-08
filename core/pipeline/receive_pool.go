package pipeline

import (
	"sync"
	"sync/atomic"

	"github.com/raralabs/canal/core/message"
)

// An IProcessorForReceiver is a lite version of IProcessor that is designed for the IReceivePool. IProcessor can also
// be passed, wherever IProcessorForReceiver can be passed, to achieve the same result.
type IProcessorForReceiver interface {
	IProcessorCommon

	IProcessorSender
}

// IReceivePool defines the interface for a receive pool in a stage. A receive pool is responsible for collecting
// messages from various processors and passing it to the processor pool. The receive pool collects message, along with
// their route information.
type IReceivePool interface {
	// addReceiveFrom registers a Processor as a sendChannel from which messages is to be received.
	addReceiveFrom(processor IProcessorForReceiver)

	// lock ...
	lock()

	// loop starts the collection of messages from various processors that have been registered to the receivePool and
	//streams the messages to the processor pool.
	loop(pool IProcessorPool)

	// isRunning checks if a receive pool is running.
	isRunning() bool

	// error sends the errors produced during the execution to appropriate channels.
	error(code uint8, text string)
}

// A receivePool pools messages from all the processors. The receivePool is
// connected to, and streams the messages to 'Receiver' sendChannel.
// It implements the IReceivePool interface.
type receivePool struct {
	stage       *stage                  // Stage that the receive pool belongs to
	receiveFrom []IProcessorForReceiver // processors from which messages is to be collected
	errorSender chan<- message.Msg      // error channel
	runLock     atomic.Value            //
}

// newReceiverPool creates a new receivePool
func newReceiverPool(stage *stage) *receivePool {
	return &receivePool{
		stage:       stage,
		receiveFrom: []IProcessorForReceiver{},
		errorSender: stage.pipeline.errorReceiver,
	}
}

// addReceiveFrom registers a Processor as a sendChannel from which messages is to be
// received.
func (rp *receivePool) addReceiveFrom(processor IProcessorForReceiver) {
	if rp.isRunning() {
		return
	}
	//loop through the processor already in receive pool
	//and raise error if processor already in pool is added
	//again
	for _, proc := range rp.receiveFrom {
		if proc == processor {
			panic("Added same Processor twice in the receiver.")
		}
	}

	rp.receiveFrom = append(rp.receiveFrom, processor)
}

// initStage initializes the receiver routes if the 'isSourceStage' is false   --->????
func (rp *receivePool) lock() {
	if rp.isRunning() {
		return
	}
	if len(rp.receiveFrom) == 0 {
		panic("receivePool should have at least one receive Processor.")
	}

	for _, processor := range rp.receiveFrom {
		if !processor.isConnected() {
			panic("Pipeline not configured correctly.")
		}
	}
}

// loop starts the collection of messages from various processors that have
// been registered to the receivePool and streams the messages to Receiver.
func (rp *receivePool) loop(pool IProcessorPool) {
	if rp.isRunning() {
		return
	}
	rp.runLock.Store(true)

	if len(rp.receiveFrom) > 0 {
		wg := sync.WaitGroup{}
		wg.Add(len(rp.receiveFrom))
		for _, proc := range rp.receiveFrom {
			rchan := proc.channelForStageId(rp.stage)
			go func() {
				for pod := range rchan {
					pool.execute(pod)
				}
				wg.Done()
			}()
		}
		wg.Wait()
	}

	//println("Receiveloop exited, closing ", rp.stage.name)
	pool.done()
}

func (rp *receivePool) isRunning() bool {
	c := rp.runLock.Load()
	return c != nil && c.(bool)
}

func (rp *receivePool) error(code uint8, text string) {
	rp.errorSender <- message.NewError(rp.stage.pipeline.id, rp.stage.id, 0, code, text)
}
