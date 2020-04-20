package core

import (
	"context"
	"github.com/raralabs/canal/core/pipeline"
	"sync"
)

// A Host hosts a number of networks with different network ids. It can host
// different networks and run them concurrently. When a network completes it's
// tasks, the host no longer tracks the network.
type Host struct {
	Id        uint                        // id of the host
	Pipelines map[uint]*pipeline.Pipeline // Pipelines stored by the host
	mutex     sync.Mutex                  //
	wg        sync.WaitGroup              //
}

// NewHost creates a new host with given id.
func NewHost(id uint) *Host {
	return &Host{
		Id:        id,
		Pipelines: make(map[uint]*pipeline.Pipeline),
	}
}

// AddPipeline adds a pipeline with given id to the host.
// Returns the created network.
func (w *Host) AddPipeline(id uint) *pipeline.Pipeline {
	w.Pipelines[id] = pipeline.NewPipeline(id)
	return w.Pipelines[id]
}

// loop starts all the pipelines and wait for them to finish. It stops tracking
// the pipeline if it has completed it's task.
func (w *Host) Start() {
	w.wg.Add(len(w.Pipelines))

	for _, pipeline := range w.Pipelines {
		pipeline.Start(context.Background(), func() {
			w.mutex.Lock()
			delete(w.Pipelines, pipeline.Id())
			w.mutex.Unlock()
			w.wg.Done()
		})
	}

	w.wg.Wait()
}
