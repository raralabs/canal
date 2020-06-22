package api

import (
	"github.com/raralabs/canal/core/pipeline"
	"reflect"
	"testing"

	"github.com/raralabs/canal/sinks"
	"github.com/raralabs/canal/sources"
	"github.com/raralabs/canal/transforms"
)

func ExpectPanic(t *testing.T, f func()) {
	defer func() {
		if r := recover(); r == nil {
			t.Fail()
		}
	}()
	f()
}

func TestNode_Traverse(t *testing.T) {
	root := NewNodeFactory().RootNode()
	src := root.SourceFrom(sources.NewInlineRange(2))
	b1 := src.Branch("One")
	b2 := src.Branch("Two")
	fanIn := b1.FanIn(b2)
	sink := fanIn.SinkTo(sinks.NewBlackholeSink())

	if !root.IsRoot() || root.NodeType != SOURCE {
		t.Fail()
	}
	if len(root.Outgoing) != 1 {
		t.Fail()
	}

	_src := root.Outgoing[0]
	branchNode := _src.ToNode

	if _src != src {
		t.Fail()
	}
	if branchNode != b1.OnNode || branchNode != b2.OnNode {
		t.Fail()
	}
	if branchNode.NodeType != BRANCH {
		t.Fail()
	}
	if len(branchNode.Outgoing) != 2 {
		t.Fail()
	}

	_b1 := branchNode.Outgoing[0]
	_fanInNode1 := _b1.ToNode
	_b2 := branchNode.Outgoing[1]
	_fanInNode2 := _b2.ToNode

	if _b1 != b1 || _b2 != b2 {
		t.Fail()
	}
	if _fanInNode1.NodeType != FANIN || _fanInNode2.NodeType != FANIN {
		t.Fail()
	}
	if _fanInNode1 != fanIn.OnNode || _fanInNode2 != fanIn.OnNode {
		t.Fail()
	}
	if len(_fanInNode1.Outgoing) != 1 {
		t.Fail()
	}

	_fanIn := _fanInNode1.Outgoing[0]
	_sinkNode := _fanIn.ToNode

	if _fanIn != fanIn {
		t.Fail()
	}
	if _sinkNode.NodeType != SINK {
		t.Fail()
	}
	if _sinkNode != sink.OnNode {
		t.Fail()
	}
	if len(_sinkNode.Outgoing) != 1 {
		t.Fail()
	}

	_sink := _sinkNode.Outgoing[0]
	if _sink != sink {
		t.Fail()
	}
	if _sink.ToNode != nil {
		t.Fail()
	}
}

func TestNode_New(t *testing.T) {
	node := NewNodeFactory().RootNode()
	if node.Id != 1 || node.ExecutorType() != pipeline.SOURCE {
		t.Fail()
	}

	node1 := node.extendNode(FANIN)
	if node1.Id != 2 || node1.ExecutorType() != pipeline.TRANSFORM {
		t.Fail()
	}

	node2 := node.extendNode(SINK)
	if node2.Id != 3 || node2.ExecutorType() != pipeline.SINK {
		t.Fail()
	}

	ExpectPanic(t, func() {
		node2.extendNode(SINK)
	})

	node3 := node1.extendNode(FANIN)
	if node3.Id != 4 {
		t.Fail()
	}
}

func TestNode_From(t *testing.T) {
	ExpectPanic(t, func() {
		node := NewNodeFactory().RootNode()
		node.SourceFrom(transforms.PassFunction())
	})

	edge := NewNodeFactory().RootNode().SourceFrom(sources.NewInlineRange(1))
	e := reflect.TypeOf(*edge)
	if e != reflect.TypeOf(Edge{}) {
		t.Fail()
	}
}

func TestSend_To(t *testing.T) {
	node := NewNodeFactory().RootNode()
	ExpectPanic(t, func() {
		node.addEdge(sinks.NewBlackholeSink())
	})

	node.addEdge(sources.NewInlineRange(1))
}
