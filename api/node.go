package api

import (
	"github.com/raralabs/canal/core/pipeline"
)

type NodeType int8

const (
	SOURCE NodeType = iota + 1
	FANIN
	FANOUT

	BRANCH
	FILTER
	COUNT
	JOIN
	TENGO
	MILOLUA
	GOPHERLUA

	GROUP
	WINDOW

	SINK
)

type Node struct {
	Id           uint
	Incoming     []*Edge
	Outgoing     []*Edge
	NodeType     NodeType
	ShortCircuit bool
	EdgeCounter  uint
	getNodeId    func() uint
}

func NewNode(nodeType NodeType, getNodeId func() uint) *Node {
	ss := false
	if nodeType == BRANCH {
		ss = true
	}

	return &Node{
		Id:           getNodeId(),
		NodeType:     nodeType,
		ShortCircuit: ss,
		EdgeCounter:  0,
		getNodeId:    getNodeId,
	}
}

func (n *Node) extendNode(nodeType NodeType) *Node {
	if n.NodeType == SINK {
		panic("Sink nodes can't extend.")
	}
	return NewNode(nodeType, n.getNodeId)
}

func (n *Node) receiveFrom(edges []*Edge) {
	for _, edge := range edges {
		edge.connect(n)
		n.Incoming = append(n.Incoming, edge)
	}
}

func (n *Node) addEdge(exec pipeline.Executor) *Edge {
	if n.ExecutorType() != exec.ExecutorType() {
		panic("stage type and executor ValueType do not match.")
	}

	n.EdgeCounter++
	edge := &Edge{
		Id:       n.EdgeCounter,
		OnNode:   n,
		Executor: exec,
		ToNode:   nil,
	}

	n.Outgoing = append(n.Outgoing, edge)
	return edge
}

func (n *Node) IsRoot() bool {
	return n.Id == 1
}

func (n *Node) SourceFrom(src pipeline.Executor) *Edge {
	if src.ExecutorType() != pipeline.SOURCE {
		panic("executor ValueType should be SOURCE.")
	}
	return n.addEdge(src)
}

func (n *Node) Multiplexed() bool {
	return n.NodeType == BRANCH || n.NodeType == FANOUT
}

func (n *Node) ExecutorType() pipeline.ExecutorType {
	if n.NodeType == SOURCE {
		return pipeline.SOURCE
	}
	if n.NodeType == SINK {
		return pipeline.SINK
	}

	return pipeline.TRANSFORM
}

type NodeFactory struct {
	NodeCounter uint
}

func NewNodeFactory() *NodeFactory {
	return &NodeFactory{NodeCounter: 0}
}

func (nf *NodeFactory) RootNode() *Node {
	return NewNode(SOURCE, nf.nextNodeId)
}

func (nf *NodeFactory) nextNodeId() uint {
	nf.NodeCounter++
	return nf.NodeCounter
}
