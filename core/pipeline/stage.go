package pipeline

import (
	"context"
	"fmt"
	"github.com/raralabs/canal/core/message"
	"sync/atomic"
)

type msgRoutes map[string]struct{}

// A stage represents an entity that is responsible for collecting messages from
// multiple other hubs and send the msg to the processorPool. It is also
// responsible for creating new jobs. A stage can be thought up as a complete
// node of a graph of the stream or the network.
type stage struct {
	id            uint32             // id of the stage
	name          string             //
	pipeline      *Pipeline          // id of the network that the hub lies in
	executorType  ExecutorType       // ValueType of transforms held by the hub
	errorSender   chan<- message.Msg //
	receivePool   receivePool        // receivePool associated with the hub
	processorPool processorPool      // processorPool associated with the hub
	routes        msgRoutes          // The incoming msg routes for the stage
	withTrace     bool               // If the messages originating from this stage should have trace enabled
	runLock       atomic.Value       // If the stage is runLock now
}

func (s *stage) GetId() uint32 {
	return s.id
}

// ReceiveFrom associates the processors from other stages with the
// receivePool of this stage. Returns the stage after associating it with the Processor.
// This function can't be called on a stage of type SOURCE, since a source does
// not receive messages from any other stages.
func (s *stage) ReceiveFrom(route string, processors ...*Processor) *stage {
	if s.runLock.Load().(bool) {
		return nil
	}

	if s.executorType == SOURCE {
		panic("Source nodes cannot receive messages.")
	}

	// add all the processors from other stages to the receivePool and connect the
	// Processor to this s
	for _, processor := range processors {
		if s.pipeline.id != processor.pipelineId {
			panic("Cannot connect processors of different networks.")
		}
		if s.id == processor.stageId {
			panic("Can't connect processors of the same source.")
		}

		s.receivePool.addReceiveFrom(processor)
		processor.addSendTo(s, route)
	}

	if _, ok := s.routes[route]; !ok {
		s.routes[route] = struct{}{}
	}

	return s
}

// add creates a new Processor in a stage with the executor
// and adds it to the processorPool of the Stage. Returns the Processor that was created.
func (s *stage) AddProcessor(executor Executor, routes ...string) *Processor {
	if s.runLock.Load().(bool) {
		panic("error")
	}

	if s.executorType != executor.ExecutorType() {
		panic("executor ExecutorType and s ExecutorType do not match.")
	}
	if s.executorType == SOURCE && len(routes) != 0 {
		panic("Source stages cannot have 'routeName' defined for executor, they don't receive messages.")
	}

	routeMap := make(msgRoutes)
	for _, route := range routes {
		if _, ok := routeMap[route]; ok {
			panic("Duplicate 'routeName' for Executor")
		}

		routeMap[route] = struct{}{}
	}

	return s.processorPool.add(executor, routeMap)
}

func (s *stage) ShortCircuit() *stage {
	if s.runLock.Load().(bool) {
		return nil
	}

	s.processorPool.shortCircuitProcessors()
	return s
}

func (s *stage) Trace() *stage {
	if s.runLock.Load().(bool) {
		return nil
	}

	if s.executorType != SOURCE {
		panic("traceFlag can be enabled only in SOURCE nodes.")
	}

	s.withTrace = true
	return s
}

// initStage initializes the stage.
func (s *stage) lock() {
	if s.runLock.Load().(bool) {
		return
	}

	s.processorPool.lock(s.routes)

	if s.executorType != SOURCE {
		s.receivePool.lock()
	}
}

// In case of SOURCE nodes, since it does not have any receivePool, we will have to run
// an infinite loop. It returns if the context timeouts or if the processorPool return true
// in its process function, which signifies that all the processors are DONE sending the msg
func (s *stage) srcLoop(c context.Context, pool processorPool) {
sourceLoop:
	for {
		select {
		case <-c.Done():
			s.error(1, "Source Timeout")
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
func (s *stage) loop(ctx context.Context, onComplete func()) {
	if s.runLock.Load().(bool) {
		return
	}

	s.runLock.Store(true)

	if s.executorType == SOURCE {
		// If its a source Stage, run srcLoop. Context sent only to source, it will cascade.
		s.srcLoop(ctx, s.processorPool)
	} else {
		// Else runs receivePool.loop to receive and process new messages
		s.receivePool.loop(s.processorPool)
	}

	// Finally call the onComplete callback
	if onComplete != nil {
		onComplete()
	}
	println("Closed stage ", s.name)
}

func (s *stage) error(code uint8, text string) {
	s.errorSender <- message.NewError(s.pipeline.Id(), s.GetId(), 0, code, text)
}

func (s *stage) isClosed() bool {
	return s.processorPool.isClosed()
}

func (s *stage) String() string {
	return fmt.Sprintf("s{id:%d type:%s}", s.id, s.executorType.String())
}

type stageFactory struct {
	pipeline *Pipeline
	hwm      uint32
}

func newStageFactory(pipeline *Pipeline) stageFactory {
	return stageFactory{pipeline: pipeline, hwm: 0}
}

func (sf *stageFactory) new(name string, executorType ExecutorType) *stage {
	stageId := atomic.AddUint32(&sf.hwm, 1)

	s := &stage{
		id:            stageId,
		name:          name,
		pipeline:      sf.pipeline,
		executorType:  executorType,
		errorSender:   sf.pipeline.errorReceiver,
		processorPool: newProcessorPool(sf.pipeline, stageId),
		routes:        make(msgRoutes),
		withTrace:     false,
	}
	if executorType != SOURCE {
		s.receivePool = newReceiverPool(sf.pipeline, stageId)
	}

	s.runLock.Store(false)

	return s
}
