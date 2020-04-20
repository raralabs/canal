package graph

import (
	"gonum.org/v1/gonum/graph"
	"gonum.org/v1/gonum/graph/encoding"
)

// Edge is a simple graph edge.
type Edge struct {
	F, T     graph.Node
	fromPort string
	toPort   string
}

// From returns the from-node of the edge.
func (e Edge) From() graph.Node { return e.F }

// To returns the to-node of the edge.
func (e Edge) To() graph.Node { return e.T }

// ReversedLine returns a new Edge with the F and T fields
// swapped.
func (e Edge) ReversedEdge() graph.Edge {
	e.F, e.T = e.T, e.F
	e.fromPort, e.toPort = e.toPort, e.fromPort
	return e
}

// Attributes returns the DOT attributes of the edge.
func (e Edge) Attributes() []encoding.Attribute {
	// if len(e.Label) == 0 {
	// 	return nil
	// }
	return []encoding.Attribute{{
		Key:   "label",
		Value: "",
	}}
}

func (e *Edge) SetFromPort(nameId string) {
	e.fromPort = nameId
}

func (e Edge) FromPort() (string, string) {
	return e.fromPort, ""
}

func (e Edge) ToPort() (string, string) {
	return "", ""
}
