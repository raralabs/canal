package api

import (
	"github.com/n-is/canal/core/pipeline"
	"testing"

	"github.com/n-is/canal/sinks"
	"github.com/n-is/canal/sources"
)

func TestEdge_To(t *testing.T) {
	root := NewNodeFactory().RootNode()
	src := root.SourceFrom(sources.NewInlineRange(1))
	ExpectPanic(t, func() {
		src.SinkTo(sources.NewInlineRange(1))
	})

	sink := src.SinkTo(sinks.NewBlackholeSink())
	if sink.OnNode.ExecutorType() != pipeline.SINK {
		t.Fail()
	}
}

func TestEdge_FanIn(t *testing.T) {
	root := NewNodeFactory().RootNode()
	src1 := root.SourceFrom(sources.NewInlineRange(1))
	src2 := root.SourceFrom(sources.NewInlineRange(1))
	src3 := root.SourceFrom(sources.NewInlineRange(1))

	fanIn := src1.FanIn(src2, src3)
	fanInNode := fanIn.OnNode

	if fanInNode.NodeType != FANIN {
		t.Fail()
	}
	if len(fanInNode.Incoming) != 3 {
		t.Fail()
	}
}

func TestEdge_FanOut(t *testing.T) {
	root := NewNodeFactory().RootNode()
	src := root.SourceFrom(sources.NewInlineRange(1))

	fanout1 := src.FanOut()
	fanout2 := src.FanOut()
	fanout3 := src.FanOut()

	if fanout1.OnNode != fanout2.OnNode || fanout2.OnNode != fanout3.OnNode {
		t.Fail()
	}

	fanoutNode := fanout1.OnNode
	if fanoutNode.NodeType != FANOUT {
		t.Fail()
	}

	if len(fanoutNode.Outgoing) != 3 {
		t.Fail()
	}

	_fanout1 := fanoutNode.Outgoing[0]
	_fanout2 := fanoutNode.Outgoing[1]
	_fanout3 := fanoutNode.Outgoing[2]

	if _fanout1 != fanout1 || _fanout2 != fanout2 || _fanout3 != fanout3 {
		t.Fail()
	}
}

func TestEdge_Branch(t *testing.T) {
	root := NewNodeFactory().RootNode()
	src := root.SourceFrom(sources.NewInlineRange(1))

	branch1 := src.Branch("one")
	branch2 := src.Branch("two")
	branch3 := src.Branch("three")

	if branch1.OnNode != branch2.OnNode || branch2.OnNode != branch3.OnNode {
		t.Fail()
	}

	branchNode := branch1.OnNode
	if branchNode.NodeType != BRANCH {
		t.Fail()
	}

	if len(branchNode.Outgoing) != 3 {
		t.Fail()
	}

	_branch1 := branchNode.Outgoing[0]
	_branch2 := branchNode.Outgoing[1]
	_branch3 := branchNode.Outgoing[2]

	if _branch1 != branch1 || _branch2 != branch2 || _branch3 != branch3 {
		t.Fail()
	}
}
