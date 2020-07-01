package core

import "github.com/raralabs/canal/core/pipeline"

// A Worker is a wrapper around host that is responsible for adding various
// networks to a host and lock them.
// The worker can also run a host as a service.
type Worker struct {
	Id   uint
	host *Host
}

// NewWorker creates a new worker with the given id.
func NewWorker(id uint) *Worker {
	return &Worker{Id: id, host: NewHost(id)}
}

// AddPipeline adds a new network with the given id to the host.
// Returns the created network.
func (w *Worker) AddPipeline(id uint) *pipeline.Pipeline {
	return w.host.AddPipeline(uint32(id))
}

// loop starts the host. If 'asService' is true, the host runs as a service.
func (w *Worker) Start(asService bool) {
	for {
		w.host.Start()

		if !asService {
			break
		}
	}
}
