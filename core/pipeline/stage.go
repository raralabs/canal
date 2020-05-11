package pipeline

import (
	"context"
	"fmt"
	"github.com/raralabs/canal/core/message"
	"sync/atomic"
)

type msgRoutes map[string]struct{}

// A stage represents an entity that is responsible for collecting messages from
// multiple other hubs and send the msg to the procPool. It is also
// responsible for creating new jobs. A stage can be thought up as a complete
// node of a graph of the stream or the network.
type stage struct {
	id            uint32             // id of the stage
	name          string             //
	pipeline      *Pipeline          // id of the pipeline that the stage lies in
	executorType  ExecutorType       // ValueType of transforms held by the hub
	errorSender   chan<- message.Msg //
	receivePool   receivePool        // receivePool associated with the hub
	processorPool processorPool      // procPool associated with the hub
	routes        msgRoutes          // The incoming msg routes for the stage
	withTrace     bool               // If the messages originating from this stage should have trace enabled
	runLock       atomic.Value       // If the stage is runLock now
}

func (stg *stage) GetId() uint32 {
	return stg.id
}

// ReceiveFrom associates the processors from other stages with the
// receivePool of this stage. Returns the stage after associating it with the Processor.
// This function can't be called on a stage of type SOURCE, since a source does
// not receive messages from any other stages.
func (stg *stage) ReceiveFrom(route string, processors ...*Processor) *stage {
	if stg.isRunning() {
		return nil
	}

	if stg.executorType == SOURCE {
		panic("Source nodes cannot receive messages.")
	}

	// add all the processors from other stages to the receivePool and connect the
	// Processor to this stg
	for _, processor := range processors {
		if stg.pipeline != processor.procPool.stage.pipeline {
			panic("Cannot connect processors of different networks.")
		}
		if stg == processor.procPool.stage {
			panic("Can't connect processors of the same source.")
		}

		stg.receivePool.addReceiveFrom(processor)
		processor.addSendTo(stg, route)
	}

	if _, ok := stg.routes[route]; !ok {
		stg.routes[route] = struct{}{}
	}

	return stg
}

// add creates a new Processor in a stage with the executor
// and adds it to the procPool of the Stage. Returns the Processor that was created.
func (stg *stage) AddProcessor(executor Executor, routes ...string) *Processor {
	if stg.isRunning() {
		panic("error")
	}

	if stg.executorType != executor.ExecutorType() {
		panic("executor ExecutorType and stg ExecutorType do not match.")
	}
	if stg.executorType == SOURCE && len(routes) != 0 {
		panic("Source stages cannot have 'routeName' defined for executor, they don't receive messages.")
	}

	routeMap := make(msgRoutes)
	for _, route := range routes {
		if _, ok := routeMap[route]; ok {
			panic("Duplicate 'routeName' for Executor")
		}

		routeMap[route] = struct{}{}
	}

	return stg.processorPool.add(executor, routeMap)
}

func (stg *stage) ShortCircuit() *stage {
	if stg.isRunning() {
		return nil
	}

	stg.processorPool.shortCircuitProcessors()
	return stg
}

func (stg *stage) Trace() *stage {
	if stg.isRunning() {
		return nil
	}

	if stg.executorType != SOURCE {
		panic("traceFlag can be enabled only in SOURCE nodes.")
	}

	stg.withTrace = true
	return stg
}

// initStage initializes the stage.
func (stg *stage) lock() {
	if stg.isRunning() {
		return
	}

	stg.processorPool.lock(stg.routes)

	if stg.executorType != SOURCE {
		stg.receivePool.lock()
	}
}

// In case of SOURCE nodes, since it does not have any receivePool, we will have to run
// an infinite loop. It returns if the context timeouts or if the procPool return true
// in its process function, which signifies that all the processors are DONE sending the msg
func (stg *stage) srcLoop(c context.Context, pool processorPool) {
sourceLoop:
	for {
		select {
		case <-c.Done():
			stg.error(1, "Source Timeout")
			break sourceLoop
		default:
			pool.execute(msgPod{})
			if pool.isClosed() {
				break sourceLoop
			}
		}
	}
}

// loop starts the execution of the stage. The 'doneCallback' function is called
// after the stage has finished execution. The stage finishes it's execution if all
// the processors associated with the stage have emitted Close Message.
func (stg *stage) loop(ctx context.Context, onComplete func()) {
	if stg.isRunning() || stg.isClosed() {
		return
	}

	stg.runLock.Store(true)

	if stg.executorType == SOURCE {
		// If its a source Stage, run srcLoop. Context sent only to source, it will cascade.
		stg.srcLoop(ctx, stg.processorPool)
	} else {
		// Else runs receivePool.loop to receive and process new messages
		stg.receivePool.loop(stg.processorPool)
	}

	// Finally call the onComplete callback
	if onComplete != nil {
		onComplete()
	}
	println("Closed stage ", stg.name)
}

func (stg *stage) error(code uint8, text string) {
	stg.errorSender <- message.NewError(stg.pipeline.Id(), stg.GetId(), 0, code, text)
}

func (stg *stage) isClosed() bool {
	return stg.processorPool.isClosed()
}

func (stg *stage) isRunning() bool {
	r := stg.runLock.Load()
	return r != nil && r.(bool)
}

func (stg *stage) String() string {
	return fmt.Sprintf("stg{id:%d type:%stg}", stg.id, stg.executorType.String())
}

type stageFactory struct {
	pipeline *Pipeline
	hwm      uint32
}

func newStageFactory(pipeline *Pipeline) stageFactory {
	return stageFactory{pipeline: pipeline, hwm: 0}
}

func (sf *stageFactory) new(name string, executorType ExecutorType) *stage {
	s := &stage{
		id:            atomic.AddUint32(&sf.hwm, 1),
		name:          name,
		pipeline:      sf.pipeline,
		executorType:  executorType,
		errorSender:   sf.pipeline.errorReceiver,
		routes:        make(msgRoutes),
		withTrace:     false,
	}

	s.processorPool = newProcessorPool(s)
	if executorType != SOURCE {
		s.receivePool = newReceiverPool(s)
	}

	return s
}
