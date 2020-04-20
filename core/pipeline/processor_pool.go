package pipeline

import (
	"github.com/raralabs/canal/core/message"
	"sync/atomic"
)

// A processorPool collects data from multiple jobs and send them to their respective
// Sender sendChannel so that the other receivePool can connect to the Sender
// sendChannel. It executes all the processors that it holds.
type processorPool struct {
	stage            *stage                //
	processors       map[uint32]*Processor //
	shortCircuit     bool                  //
	processorFactory processorFactory      //
	runLock          atomic.Value          //
	closed           atomic.Value          //
}

// newProcessorPool creates a new processorPool with default values.
func newProcessorPool(stage *stage) processorPool {
	return processorPool{
		stage:            stage,
		shortCircuit:     false,
		processors:       make(map[uint32]*Processor),
		processorFactory: newProcessorFactory(stage),
	}
}

func (pool *processorPool) shortCircuitProcessors() {
	if pool.isRunning() {
		return
	}
	pool.shortCircuit = true
}

// add adds a Processor to the list of processors to be executed
func (pool *processorPool) add(executor Executor, routes msgRoutes) *Processor {
	if pool.isRunning() {
		return nil
	}

	processor := pool.processorFactory.new(executor, routes)
	pool.processors[processor.id] = processor

	return processor
}

// initStage initializes the processorPool and checks if all the jobs registered to the
// processorPool has been properly connected or not.
func (pool *processorPool) lock(stgRoutes msgRoutes) {
	if pool.isRunning() {
		return
	}
	if len(pool.processors) == 0 {
		panic("processorPool should have at least one Processor.")
	}

	for _, processor := range pool.processors {
		processor.lock(stgRoutes)
	}
	pool.runLock.Store(true)
}

// process executes the corresponding process on all the processors with the same msg 'm'
func (pool *processorPool) execute(pod msgPod) {
	if pool.isClosed() || !pool.isRunning() {
		return
	}

	allClosed := true
	for _, processor := range pool.processors {
		if processor.isClosed() {
			continue
		}

		if processor.executor.ExecutorType() == SOURCE {
			pod = newMsgPod(processor.statusMessage(pool.stage.withTrace))
		}

		accepted := processor.process(pod)
		if !processor.isClosed() {
			allClosed = false
		}

		if accepted && pool.shortCircuit {
			break
		}
	}

	if allClosed {
		pool.close()
		println("All processors closed, closed processorpool ", pool.stage.name)
	}
}

func (pool *processorPool) close() {
	for _, processor := range pool.processors {
		if !processor.isClosed() {
			processor.Close()
		}
	}
	pool.closed.Store(true)
}

func (pool *processorPool) isRunning() bool {
	r := pool.runLock.Load()
	return r != nil && r.(bool)
}

func (pool *processorPool) isClosed() bool {
	c := pool.closed.Load()
	return c != nil && c.(bool)
}

// A processorFactory represents a factory that can produce processors(s).
type processorFactory struct {
	stage *stage
	hwm   uint32 // hwm is used to provide id to the transforms. It is incremented each time a new transforms is created.
}

// newProcessorFactory creates a new transforms producing factory.
func newProcessorFactory(stage *stage) processorFactory {
	// Multiply by 1000 just to ensure that processorId remain different for different processors in different stages
	return processorFactory{stage: stage, hwm: stage.id * 1000}
}

// NewExecute creates a new transforms that is to be connected to 'hub' and 'executor' as the
// main executing entity and returns it.
func (factory *processorFactory) new(executor Executor, routeMap msgRoutes) *Processor {
	processorId := atomic.AddUint32(&factory.hwm, 1)
	stage := factory.stage

	p := &Processor{
		id:          processorId,
		processorPool: &factory.stage.processorPool,
		executor:    executor,
		routes:      routeMap,
		errorSender: factory.stage.pipeline.errorReceiver,
		mesFactory:  message.NewFactory(factory.stage.pipeline.id, stage.id, processorId),
	}

	if stage.executorType != SINK {
		p.sendPool = newSendPool(p)
	}

	return p
}
