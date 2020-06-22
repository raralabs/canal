package api

import (
	"errors"
	"github.com/raralabs/canal/core/message"
	"github.com/raralabs/canal/core/pipeline"

	"github.com/Knetic/govaluate"
	"github.com/raralabs/canal/transforms"
)

type Edge struct {
	Id       uint
	OnNode   *Node
	Executor pipeline.Executor
	ToNode   *Node
}

func (edge *Edge) connect(vertex *Node) {
	if edge.ToNode != nil {
		panic("A job can be connected only once.")
	}
	edge.ToNode = vertex
}

func (edge *Edge) buildNextNode(nodeType NodeType, receivers ...*Edge) *Node {
	var node *Node

	if edge.ToNode != nil {
		if edge.ToNode.NodeType == nodeType && edge.ToNode.Multiplexed() {
			return edge.ToNode
		}

		panic("Cant send output from the same node twice.")
	}

	node = edge.OnNode.extendNode(nodeType)
	node.receiveFrom(append(receivers, edge))

	return node
}

func (edge *Edge) SinkTo(sink pipeline.Executor) *Edge {
	if sink.ExecutorType() != pipeline.SINK {
		panic("SinkTo only accepts SINK type of jobs.")
	}

	node := edge.buildNextNode(SINK)
	return node.addEdge(sink)
}

func (edge *Edge) FanIn(edges ...*Edge) *Edge {
	node := edge.buildNextNode(FANIN, edges...)
	return node.addEdge(transforms.PassFunction())
}

func (edge *Edge) FanOut() *Edge {
	node := edge.buildNextNode(FANOUT)
	return node.addEdge(transforms.PassFunction())
}

func (edge *Edge) Filter(condition string) *Edge {
	expression, err := govaluate.NewEvaluableExpression(condition)
	if err != nil {
		panic(err)
	}

	filter := func(m *message.Msg) (bool, error) {
		ret := false

		result, err := expression.Evaluate(m.Values())
		if err != nil {
			return false, err
		}

		rbool, ok := result.(bool)
		if !ok {
			return false, errors.New("non boolean result in filter condition")
		} else {
			ret = rbool
		}

		return ret, nil
	}

	node := edge.buildNextNode(FILTER)
	return node.addEdge(transforms.FilterFunction(filter))
}

func (edge *Edge) Branch(condition string) *Edge {
	expression, err := govaluate.NewEvaluableExpression(condition)
	if err != nil {
		panic(err)
	}

	filter := func(m *message.Msg) (bool, error) {
		ret := false

		result, err := expression.Evaluate(m.Values())
		if err != nil {
			return false, err
		}

		rbool, ok := result.(bool)
		if !ok {
			return false, errors.New("non boolean result in filter condition")
		} else {
			ret = rbool
		}

		return ret, nil
	}

	node := edge.buildNextNode(BRANCH)
	return node.addEdge(transforms.FilterFunction(filter))
}

func (edge *Edge) BranchDefault() *Edge {
	return edge.Branch("true")
}

func (edge *Edge) Tengo(src []byte) *Edge {
	node := edge.buildNextNode(TENGO)
	tengo := transforms.NewTengo(src)
	return node.addEdge(tengo.TengoFunction())
}

func (edge *Edge) GopherLua(src []byte) *Edge {
	node := edge.buildNextNode(GOPHERLUA)
	azureLua := transforms.NewGopherLua(src)
	return node.addEdge(azureLua.GopherLuaFunction())
}
