package pipeline

import (
	"sync/atomic"
)

type IProcessorForPool interface {
	IProcessorCommon

	IProcessorReceiver
}

type IProcessorPool interface {
	add(executor Executor, routes msgRoutes) IProcessor

	shortCircuitProcessors()

	lock(stgRoutes msgRoutes)

	isClosed() bool

	isRunning() bool

	stage() *stage

	attach(...IProcessorForPool)

	detach(...IProcessorForPool)

	execute(pod msgPod)

	error(uint8, error)

	done()
}

// A procPool collects data from multiple jobs and send them to their respective
// Sender sendChannel so that the other receivePool can connect to the Sender
// sendChannel. It executes all the processors that it holds.
type processorPool struct {
	stg              *stage           //
	shortCircuit     bool             //
	processorFactory processorFactory //
	runLock          atomic.Value     //
	closed           atomic.Value     //

	procMsgPaths map[msgRouteParam][]IProcessorForPool // Maps the incoming route to a list of processors that subscribe to the path
}

// newProcessorPool creates a new procPool with default values.
func newProcessorPool(stage *stage) *processorPool {

	procMsgPaths := make(map[msgRouteParam][]IProcessorForPool)

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
func (pool *processorPool) add(executor Executor, routes msgRoutes) IProcessor {
	if pool.isRunning() {
		return nil
	}

	processor := pool.processorFactory.new(executor, routes)
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
		panic("procPool should have at least one Processor.")
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
		if path != msgRouteParam("") && path != pod.route {
			continue
		}

		for _, proc := range procs {
			if proc.isClosed() {
				continue
			}

			accepted := proc.process(pod.msg)
			if !proc.isClosed() {
				allClosed = false
			}

			if accepted && pool.shortCircuit {
				break
			}
		}
	}

	if allClosed {
		pool.done()
		println("All processors closed, closed processorpool ", pool.stg.name)
	}
}

func (pool *processorPool) error(uint8, error) {
}

func (pool *processorPool) done() {
	for _, processors := range pool.procMsgPaths {
		for _, processor := range processors {
			if !processor.isClosed() {
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
