package pipeline

import (
	"context"
	"fmt"
	"github.com/raralabs/canal/core/message"
	"sync"
	"sync/atomic"
)

// A Pipeline represents a group of stages that are connected among themselves
// according to some specification.
type Pipeline struct {
	id            uint32           // id of the network
	stages        []*stage         // stages stored by the network
	errorReceiver chan message.Msg // A receiver to receive all the errors in the network after execution
	stageFactory  stageFactory     // Factory to create new Stages
	runLock       atomic.Value     // signifies if the Pipeline is runLock
	wg            sync.WaitGroup   // For waiting all Stages to finish
}

// NewPipeline creates a new network with the given id.
func NewPipeline(id uint32) *Pipeline {
	p := &Pipeline{
		id:            id,
		stages:        []*stage{},
		errorReceiver: make(chan message.Msg, 100),
	}

	p.stageFactory = newStageFactory(p)
	p.runLock.Store(false)
	return p
}

func (pl *Pipeline) Id() uint32 {
	return pl.id
}

// AddSource adds a SOURCE stg to the network.
func (pl *Pipeline) AddSource(name string) *stage {
	if pl.runLock.Load().(bool) {
		return nil
	}

	stg := pl.stageFactory.new(name, SOURCE)
	pl.stages = append(pl.stages, stg)
	return stg
}

// AddTransform adds a TRANSFORM stg to the network.
func (pl *Pipeline) AddTransform(name string) *stage {
	if pl.runLock.Load().(bool) {
		return nil
	}

	stg := pl.stageFactory.new(name, TRANSFORM)
	pl.stages = append(pl.stages, stg)
	return stg
}

// AddSink adds a SINK stg to the network
func (pl *Pipeline) AddSink(name string) *stage {
	if pl.runLock.Load().(bool) {
		return nil
	}

	stg := pl.stageFactory.new(name, SINK)
	pl.stages = append(pl.stages, stg)
	return stg
}

// runLock initializes a Pipeline and checks that the Pipeline is configured correctly and is ready for execution.
func (pl *Pipeline) Validate() {
	if pl.runLock.Load().(bool) {
		return
	}

	// Initialize so check if the Stages are configured correctly, if not panic
	for _, stage := range pl.stages {
		stage.lock()
	}
}

// Start initializes and starts all the stages in the Pipeline as go routines.
func (pl *Pipeline) Start(ctx context.Context, done func()) {
	if pl.runLock.Load().(bool) {
		return
	}

	// Once started, lets runLock the Pipeline
	pl.runLock.Store(true)

	go func() {
		for msg := range pl.errorReceiver {
			fmt.Println(fmt.Sprintf("[ERROR] %s", msg.String()))
		}
	}()

	// Every stg runs its own goroutine, we should wait for all to finish
	pl.wg.Add(len(pl.stages))
	for _, stage := range pl.stages {
		go stage.loop(ctx, pl.wg.Done)
	}
	pl.wg.Wait()

	if done != nil {
		done()
	}
	close(pl.errorReceiver)
}
