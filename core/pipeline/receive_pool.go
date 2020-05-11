package pipeline

import (
	"github.com/raralabs/canal/core/message"
	"sync"
	"sync/atomic"
)

// A receivePool pools messages from all the processors the receivePool is
// connected to, and streams the messages to 'Receiver' sendChannel.
type receivePool struct {
	stage       *stage             //
	receiveFrom []*Processor       //
	errorSender chan<- message.Msg //
	runLock     atomic.Value       //
}

// newReceiverPool creates a new receivePool
func newReceiverPool(stage *stage) receivePool {
	return receivePool{
		stage:       stage,
		receiveFrom: []*Processor{},
		errorSender: stage.pipeline.errorReceiver,
	}
}

// addReceiveFrom registers a Processor as a sendChannel from which messages is to be
// received.
func (rp *receivePool) addReceiveFrom(processor *Processor) {
	if rp.isRunning() {
		return
	}
	for _, proc := range rp.receiveFrom {
		if proc == processor {
			panic("Added same Processor twice in the receiver.")
		}
	}

	rp.receiveFrom = append(rp.receiveFrom, processor)
}

// initStage initializes the receiver routes if the 'isSourceStage' is false
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
func (rp *receivePool) loop(pool processorPool) {
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

	println("Receiveloop exited, closing ", pool.stage.name)
	pool.close()
}

func (rp *receivePool) isRunning() bool {
	c := rp.runLock.Load()
	return c != nil && c.(bool)
}

func (rp *receivePool) error(code uint8, text string) {
	rp.errorSender <- message.NewError(rp.stage.pipeline.id, rp.stage.id, 0, code, text)
}
