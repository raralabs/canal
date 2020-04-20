package network

import (
	"github.com/n-is/canal/core/pipeline"
)

// A nodeTransform represents the corresponding transforms in a node.
type nodeTransform struct {
	node *Node             // The node held by a nodeTransform
	job  pipeline.Executor // The transforms held by a nodeTransform
}

// An Edge is a basic unit of pipeline, which connects two stages in the pipeline.
type Edge struct {
	from *nodeTransform // The starting point of the edge
	to   *Node          // The end point of the edge
}

func newEdge(from *nodeTransform) *Edge {
	return &Edge{from: from, to: nil}
}

// From returns the from node of the edge.
func (e *Edge) From() *Node {
	return e.from.node
}

// From returns the from node of the edge.
func (e *Edge) FromJob() pipeline.Executor {
	return e.from.job
}

// To returns the to node of the edge.
func (e *Edge) To() *Node {
	return e.to
}
