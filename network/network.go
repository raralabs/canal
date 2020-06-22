package network

import (
	"github.com/raralabs/canal/core"
	"github.com/raralabs/canal/core/pipeline"
)

const genesisNodeId = uint(1)

// A Network is made up of stages that are connected by Edges.
type Network struct {
	id     uint    // The id of the network
	Edges  []*Edge // The edges in the network
	Nodes  []*Node // The nodes in the network
	nodeId uint    // The id generator for the nodes in the network

	pipeline       *core.pipeline // The backend pipeline
	genesisCreated bool           // shows if a genesis node have been created
}

func (n *Network) getNodeId() uint {
	id := n.nodeId
	n.nodeId++
	return id
}

// newNode creates a new node in the network with the given id and returns it.
func (n *Network) newNode(id uint) *Node {
	node := newNode(id)
	node.network = n
	n.Nodes = append(n.Nodes, node)
	return node
}

// NewNetwork creates a new network with given id and returns it.
func NewNetwork(id uint) *Network {
	// nodeId of a network starts from 1 as 0 is reserved for the source
	return &Network{id: id, nodeId: genesisNodeId + 1}
}

// GenesisNode creates a genesis node in the network and returns it.
func (n *Network) GenesisNode() *Node {
	if !n.genesisCreated {
		n.genesisCreated = true
		return n.newNode(genesisNodeId)
	}

	panic("Genesis Node already created")
}

// NodeFrom creates a new node in the network with the edges that connects to
// the node and returns the node.
func (n *Network) NodeFrom(edges ...*Edge) *Node {
	node := n.newNode(n.getNodeId())

	// add the destination of edges to the current node
	for _, e := range edges {
		e.to = node
	}

	n.Edges = append(n.Edges, edges...)
	return node
}

// Source creates a source node in the network and returns an edge from the
// source. The id of the source is 0.
func Source(job pipeline.Executor) *Node {
	if job.ExecutorType() != pipeline.SOURCE {
		panic("Non-Source job assigned to source node")
	}

	node := newNode(0)
	node.source(job)
	node.network = nil
	return node
}

// Sink creates a sink node in the network.
func (n *Network) Sink(job pipeline.Executor, edges ...*Edge) {
	snk := n.NodeFrom(edges...)
	if job.ExecutorType() != pipeline.SINK {
		panic("Non-sink job assigned to sink node")
	}

	snk.Sink(job)
}

// Build builds the network and looks for any inconsistencies in the network
func (n *Network) Build() {
	n.pipeline = pipeline.NewPipeline(n.id)

	var stages []*core.stage

	ntTransform := make(map[*Node]map[pipeline.Executor]*core.processor)

	for _, node := range n.Nodes {
		var stage *core.stage
		switch node.nodeType {
		case TRANSFORM:
			stage = n.pipeline.AddTransform(node.id)
		case SINK:
			stage = n.pipeline.AddSink(node.id)
		}

		if stage == nil {
			panic("Unknown node in the network")
		}

		ntTransform[node] = make(map[pipeline.Executor]*core.processor)
		for _, job := range node.jobs {
			tr := stage.AddExecutor(job)
			ntTransform[node][job] = tr
		}

		stages = append(stages, stage)
	}

	for _, stage := range stages {
		for _, edge := range n.Edges {
			if edge.to.id == stage.id {
				stage.ReceiveFrom(ntTransform[edge.from.node][edge.from.job])
			}
		}
	}
}

// Run runs the network by providing the sources to genesis node
func (n *Network) Run(sourceNodes ...*Node) {
	if n.pipeline == nil {
		panic("pipeline not built")
	}

	genesisHub := n.pipeline.GetStage(genesisNodeId)

	for _, sourceNode := range sourceNodes {
		if len(sourceNode.jobs) != 1 {
			panic("Source node should hold one job only")
		}
		stage := n.pipeline.AddSource(sourceNode.id)
		tr := stage.AddExecutor(sourceNode.jobs[0])
		genesisHub.ReceiveFrom(tr)
	}

	n.pipeline.Start()
	n.pipeline.Wait(func() {})

	n.pipeline.RemoveLastNodes(len(sourceNodes))
}
