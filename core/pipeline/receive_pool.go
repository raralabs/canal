package pipeline

import (
	"github.com/raralabs/canal/core/message"
	"sync"
	"sync/atomic"
)

// A receivePool pools messages from all the processors the receivePool is
// connected to, and streams the messages to 'Receiver' sendChannel.
type receivePool struct {
	pipelineId  uint32
	stageId     uint32
	receiveFrom map[uint32]*Processor
	errorSender chan<- message.Msg
	runLock     atomic.Value
	wg          sync.WaitGroup
}

// newReceiverPool creates a new receivePool
func newReceiverPool(pipeline *Pipeline, stageId uint32) receivePool {
	return receivePool{
		pipelineId:  pipeline.id,
		stageId:     stageId,
		receiveFrom: make(map[uint32]*Processor),
		errorSender: pipeline.errorReceiver,
	}
}

// addReceiveFrom registers a Processor as a sendChannel from which messages is to be
// received.
func (rp *receivePool) addReceiveFrom(processor *Processor) {
	if rp.isRunning() {
		return
	}
	if _, ok := rp.receiveFrom[processor.id]; ok {
		panic("Added same Processor twice in the receiver.")
	}

	rp.receiveFrom[processor.id] = processor
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
		rp.wg.Add(len(rp.receiveFrom))
		for _, proc := range rp.receiveFrom {
			rchan := proc.channelForStageId(rp.stageId)
			go func(){
				for pod := range rchan {
					pool.execute(pod)
				}
				rp.wg.Done()
			}()
		}
		rp.wg.Wait()
	}

	println("Receiveloop exited, closing ", pool.getStage().name)
	pool.close()
}

func (rp *receivePool) isRunning() bool {
	c := rp.runLock.Load()
	return c != nil && c.(bool)
}

func (rp *receivePool) error(code uint8, text string) {
	rp.errorSender <- message.NewError(rp.pipelineId, rp.stageId, 0, code, text)
}
