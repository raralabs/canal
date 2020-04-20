package graph

import (
	"fmt"

	"gonum.org/v1/gonum/graph/encoding"
	"gonum.org/v1/gonum/graph/simple"
)

type Node struct {
	simple.Node
	dotID    string
	label    string
	jobLabel string
}

func newNode(id int64, dotId string) Node {
	n := Node{
		Node:  simple.Node(id),
		dotID: dotId,
	}

	return n
}

// DOTID returns the node's DOT ID.
func (n Node) DOTID() string {
	return n.dotID
}

// SetDOTID sets a DOT ID.
func (n *Node) SetDOTID(id string) {
	n.dotID = id
}

// Attributes returns the DOT attributes of the node.
func (n Node) Attributes() []encoding.Attribute {
	if n.label != "" {
		return []encoding.Attribute{{
			Key:   "label",
			Value: fmt.Sprintf("{%s | {%s}}", n.label, n.jobLabel),
		}}
	}

	return nil
}

func (n *Node) SetLabel(name string) {
	n.label = fmt.Sprintf("%s\nId = %d", name, n.ID())
}

func (n *Node) AddJobLabel(label string, id int64) {
	lbl := fmt.Sprintf("<job%d> %s", id, label)
	if n.jobLabel == "" {
		n.jobLabel = fmt.Sprintf("%s", lbl)
	} else {
		n.jobLabel = fmt.Sprintf("%s | %s", n.jobLabel, lbl)
	}
}
