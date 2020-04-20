package api

import (
	"github.com/n-is/canal/core"
	pipeline2 "github.com/n-is/canal/core/pipeline"
)

type LocalWorker struct {
	StageCache map[uint]*core.stage
	Worker     *core.Worker
}

func NewLocalWorker() *LocalWorker {
	return &LocalWorker{StageCache: make(map[uint]*core.stage), Worker: core.NewWorker(1)}
}

func (w *LocalWorker) Build(node *Node) *core.pipeline {
	if !node.IsRoot() {
		panic("Worker provided non root node.")
	}

	pipeline := w.Worker.AddPipeline(1)
	w.processNode(pipeline, node)

	return pipeline
}

func (w *LocalWorker) processNode(pipeline *core.pipeline, node *Node) *core.stage {
	stage := w.buildPipelineStage(pipeline, node)

	for _, edge := range node.Outgoing {
		job := stage.AddExecutor(edge.Executor)
		if edge.ToNode != nil {
			w.processNode(pipeline, edge.ToNode).ReceiveFrom(job)
		}
	}

	return stage
}

func (w *LocalWorker) buildPipelineStage(pipeline *core.pipeline, node *Node) *core.stage {
	stage, ok := w.StageCache[node.Id]

	if !ok {
		switch node.ExecutorType() {
		case pipeline2.SOURCE:
			stage = pipeline.AddSource(node.Id)
		case pipeline2.TRANSFORM:
			stage = pipeline.AddTransform(node.Id)
		case pipeline2.SINK:
			stage = pipeline.AddSink(node.Id)
		}

		if node.ShortCircuit {
			stage.ShortCircuit()
		}
	}

	return stage
}
