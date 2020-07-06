package pipeline

import (
	"log"
	"sync/atomic"
)

// An IProcessorForPool is a lite version of IProcessor that is designed for the IProcessorPool. IProcessor can also be
// passed, wherever IProcessorForPool can be passed, to achieve the same result.
type IProcessorForPool interface {
	IProcessorCommon

	IProcessorReceiver
}

// IProcessorPool defines the interface for a processor pool. It is responsible for collecting messages from the
// receiver and routing it properly to the processors that has subscribed to the message's path. It acts as a 'Subject'
// that notifies processors about messages properly, each time it receives a message.
type IProcessorPool interface {
	// add creates an IProcessor with passed parameter and attaches it to the Processor pool.
	add(opts ProcessorOptions, executor Executor, routes msgRoutes) IProcessor

	// shortCircuitProcessors ...
	shortCircuitProcessors()

	// lock ...
	lock(stgRoutes msgRoutes)

	// IsClosed checks if the processor pool is fully closed.
	// A processor pool is fully closed when all the processors attached to it are closed.
	isClosed() bool

	// isRunning checks if the processor pool is running, i.e. it is passing the messages to the processors.
	isRunning() bool

	// stage returns the stage that the processor pool belongs to.
	stage() *stage

	// attach attaches a processor to the processor pool.
	attach(...IProcessorForPool)

	// detach detaches a processor to the processor pool.
	detach(...IProcessorForPool)

	// execute routes the msgPod to the processors that has subscribed to it.
	execute(pod msgPod)

	// error sends the errors produced during the execution to appropriate channels.
	error(uint8, error)

	// done closes all the processors that are attached to the processor pool.
	// IsClosed() should return true after a call is made to this method.
	done()
}

// A procPool collects data from multiple jobs and send them to their respective
// Sender sendChannel so that the other receivePool can connect to the Sender
// sendChannel. It executes all the processors that it holds.
// It implements the IProcessorPool interface.
type processorPool struct {
	stg              *stage           //
	shortCircuit     bool             //
	processorFactory processorFactory //
	runLock          atomic.Value     //
	closed           atomic.Value     //

	procMsgPaths map[MsgRouteParam][]IProcessorForPool // Maps the incoming route to a list of processors that subscribe to the path
}

// newProcessorPool creates a new procPool with default values.
func newProcessorPool(stage *stage) *processorPool {

	procMsgPaths := make(map[MsgRouteParam][]IProcessorForPool)

	return &processorPool{
		stg:              stage,
		shortCircuit:     false,
		processorFactory: newProcessorFactory(stage),

		procMsgPaths: procMsgPaths,
	}
}

func (pool *processorPool) stage() *stage {
	return pool.stg
}

func (pool *processorPool) shortCircuitProcessors() {
	if pool.isRunning() {
		return
	}
	pool.shortCircuit = true
}

// add adds a Processor to the list of processors to be executed
func (pool *processorPool) add(opts ProcessorOptions, executor Executor, routes msgRoutes) IProcessor {
	if pool.isRunning() {
		return nil
	}

	processor := pool.processorFactory.new(opts, executor, routes)
	pool.attach(processor)

	return processor
}

// initStage initializes the procPool and checks if all the jobs registered to the
// procPool has been properly connected or not.
func (pool *processorPool) lock(stgRoutes msgRoutes) {
	if pool.isRunning() {
		return
	}
	if len(pool.procMsgPaths) == 0 {
		panic("Processor Pool: " + pool.stage().name + " should have at least one Processor.")
	}

	for _, procs := range pool.procMsgPaths {
		for _, proc := range procs {
			proc.lock(stgRoutes)
		}
	}
	pool.runLock.Store(true)
}

// attach attaches a processor to the processor pool. It also registers the processor, so that it can receive the
// messages from the paths it has subscribed.
func (pool *processorPool) attach(procs ...IProcessorForPool) {

	for _, proc := range procs {
		procRoutes := proc.incomingRoutes()

		for path := range procRoutes {
			for _, pr := range pool.procMsgPaths[path] {
				if proc == pr {
					goto nextProc
				}
			}
			pool.procMsgPaths[path] = append(pool.procMsgPaths[path], proc)
		}
	nextProc:
	}
}

// attach detaches a processor from the processor pool. It also un-registers the processor, so that it no longer receive
// the messages from the paths it had subscribed before.
func (pool *processorPool) detach(procs ...IProcessorForPool) {

	for _, proc := range procs {
		procRoutes := proc.incomingRoutes()

		for path := range procRoutes {
			index := -1
			for i, pr := range pool.procMsgPaths[path] {
				if proc == pr {
					index = i
					break
				}
			}
			if index != -1 {
				pool.procMsgPaths[path] = removeProcRecv(pool.procMsgPaths[path], index)
			}
		}
	}
}

func removeProcRecv(pr []IProcessorForPool, i int) []IProcessorForPool {
	pr[i] = pr[len(pr)-1]
	return pr[:len(pr)-1]
}

// execute executes the corresponding execute on all the processors with the same msg 'm', that has subscribed to the
// path of msg 'm'
func (pool *processorPool) execute(pod msgPod) {
	if pool.isClosed() || !pool.isRunning() {
		return
	}

	allClosed := true

	for path, procs := range pool.procMsgPaths {
		if path != MsgRouteParam("") && path != pod.route {
			continue
		}

		for _, proc := range procs {
			if proc.IsClosed() {
				continue
			}

			accepted := proc.process(pod.msg)
			if !proc.IsClosed() {
				allClosed = false
			}

			if accepted && pool.shortCircuit {
				break
			}
		}
	}

	if allClosed {
		pool.done()
		log.Println("All processors closed, closed processorpool ", pool.stg.name)
	}
}

func (pool *processorPool) error(uint8, error) {
}

func (pool *processorPool) done() {
	for _, processors := range pool.procMsgPaths {
		for _, processor := range processors {
			if !processor.IsClosed() {
				processor.Done()
			}
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
