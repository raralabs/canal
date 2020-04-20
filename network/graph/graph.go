package graph

import (
	"fmt"
	"github.com/n-is/canal/core/pipeline"

	"github.com/n-is/canal/network"
	"gonum.org/v1/gonum/graph"
	"gonum.org/v1/gonum/graph/encoding"
	"gonum.org/v1/gonum/graph/encoding/dot"
	"gonum.org/v1/gonum/graph/simple"
)

type Graph struct {
	dg                *simple.DirectedGraph
	id                string
	graph, node, edge attributes
}

func NewGraph(net *network.Network) *Graph {

	dg := simple.NewDirectedGraph()
	g := &Graph{dg: dg}

	nodeJob := make(map[int64]map[pipeline.Executor]int64, len(net.Edges))

	edges := make(map[int64]Node)

	for _, node := range net.Nodes {

		id := int64(node.Id())
		n := newNode(id, node.Name())
		n.SetLabel(node.Name())

		nodeJob[id] = make(map[pipeline.Executor]int64)

		for i, job := range node.Jobs() {
			n.AddJobLabel(job.Name(), int64(i))
			nodeJob[id][job] = int64(i)
		}
		edges[id] = n
		g.AddNode(n)
	}

	for _, edge := range net.Edges {
		fromId := int64(edge.From().Id())
		toId := int64(edge.To().Id())

		e := &Edge{}
		e.F = edges[fromId]
		e.T = edges[toId]

		e.SetFromPort(fmt.Sprintf("%s%d", "job", nodeJob[fromId][edge.FromJob()]))
		g.SetEdge(e)
	}

	g.node.SetAttribute(encoding.Attribute{
		Key:   "shape",
		Value: "record",
	})
	g.node.SetAttribute(encoding.Attribute{
		Key:   "nodesep",
		Value: "1",
	})

	return g
}

func (g *Graph) Marshal(name, prefix, indent string) ([]byte, error) {
	return dot.Marshal(g, name, prefix, indent)
}

// AddNode adds n to the graph. It panics if the added node ID matches an existing node ID.
func (g *Graph) AddNode(n graph.Node) {
	g.dg.AddNode(n)
}

// Edge returns the edge from u to v if such an edge exists and nil otherwise.
// The node v must be directly reachable from u as defined by the From method.
func (g *Graph) Edge(uid, vid int64) graph.Edge {
	return g.dg.Edge(uid, vid)
}

// Edges returns all the edges in the graph.
func (g *Graph) Edges() graph.Edges {
	return g.dg.Edges()
}

// From returns all nodes in g that can be reached directly from n.
func (g *Graph) From(id int64) graph.Nodes {
	return g.dg.From(id)
}

// HasEdgeBetween returns whether an edge exists between nodes x and y without
// considering direction.
func (g *Graph) HasEdgeBetween(xid, yid int64) bool {
	return g.dg.HasEdgeBetween(xid, yid)
}

// HasEdgeFromTo returns whether an edge exists in the graph from u to v.
func (g *Graph) HasEdgeFromTo(uid, vid int64) bool {
	return g.dg.HasEdgeFromTo(uid, vid)
}

// NewEdge returns a new Edge from the source to the destination node.
func (g *Graph) NewEdge(from, to graph.Node) graph.Edge {
	return g.dg.NewEdge(from, to)
}

// GenesisNode returns a new unique Node to be added to g. The Node's ID does
// not become valid in g until the Node is added to g.
func (g *Graph) NewNode() graph.Node {
	return g.dg.NewNode()
}

// Node returns the node with the given ID if it exists in the graph,
// and nil otherwise.
func (g *Graph) Node(id int64) graph.Node {
	return g.dg.Node(id)
}

// stages returns all the nodes in the graph.
func (g *Graph) Nodes() graph.Nodes {
	return g.dg.Nodes()
}

// RemoveEdge removes the edge with the given end point IDs from the graph, leaving the terminal
// nodes. If the edge does not exist it is a no-op.
func (g *Graph) RemoveEdge(fid, tid int64) {
	g.dg.RemoveEdge(fid, tid)
}

// RemoveNode removes the node with the given ID from the graph, as well as any edges attached
// to it. If the node is not in the graph it is a no-op.
func (g *Graph) RemoveNode(id int64) {
	g.dg.RemoveNode(id)
}

// SetEdge adds e, an edge from one node to another. If the nodes do not exist, they are added
// and are set to the nodes of the edge otherwise.
// It will panic if the IDs of the e.From and e.To are equal.
func (g *Graph) SetEdge(e graph.Edge) {
	g.dg.SetEdge(e)
}

// To returns all nodes in g that can reach directly to n.
func (g *Graph) To(id int64) graph.Nodes {
	return g.dg.To(id)
}

// DOTAttributers implements the dot.Attributers interface.
func (g Graph) DOTAttributers() (graph, node, edge encoding.Attributer) {
	return g.graph, g.node, g.edge
}

// DOTAttributeSetters implements the dot.AttributeSetters interface.
func (g *Graph) DOTAttributeSetters() (graph, node, edge encoding.AttributeSetter) {
	return &g.graph, &g.node, &g.edge
}

// SetDOTID sets the DOT ID of the graph.
func (g *Graph) SetDOTID(id string) {
	g.id = id
}

// DOTID returns the DOT ID of the graph.
func (g *Graph) DOTID() string {
	return g.id
}

// attributes is a helper for global attributes.
type attributes []encoding.Attribute

func (a attributes) Attributes() []encoding.Attribute {
	return []encoding.Attribute(a)
}
func (a *attributes) SetAttribute(attr encoding.Attribute) error {
	*a = append(*a, attr)
	return nil
}
