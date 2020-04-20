package network

import (
	"errors"
	"fmt"
	"github.com/n-is/canal/core/message"
	"github.com/n-is/canal/core/pipeline"
	"log"

	"github.com/n-is/canal/transforms/join/table"

	"github.com/n-is/canal/transforms/agg"

	"github.com/n-is/canal/transforms/event/poll"

	"github.com/Knetic/govaluate"
	"github.com/n-is/canal/core"
	"github.com/n-is/canal/transforms"
)

// executorType represents the type of the node
type NodeType int8

const (
	NONE NodeType = iota

	SOURCE
	TRANSFORM
	SINK
)

// A Node is a fundamental unit of which a network is formed.
type Node struct {
	network   *Network            // The network to which the node belongs
	id        uint                // The id of the node
	jobs      []pipeline.Executor // The transforms that a node performs parallel on data
	nodeType  NodeType
	name      string
	edges     []*Edge
	edgeAlias map[string]*Edge
}

// newNode creates a new node with given id and returns it.
func newNode(id uint) *Node {
	return &Node{id: id, nodeType: NONE, edgeAlias: make(map[string]*Edge)}
}

func (node *Node) setNodeType(nt NodeType) {
	if node.nodeType == nt {
		return
	}

	if node.nodeType != NONE {
		panic("Can't change nodetype once it's set")
	}

	node.nodeType = nt
}

// source adds the source 'job' to the node
func (node *Node) source(job pipeline.Executor) {
	if len(node.jobs) != 0 {
		panic("Trying to add multiple jobs to a source node")
	}
	node.setNodeType(SOURCE)
	node.name = "Source"
	node.jobs = append(node.jobs, job)
}

// sink adds the sink 'job' to the node
func (node *Node) Sink(job pipeline.Executor) {
	node.setNodeType(SINK)
	node.name = "Sink"
	node.jobs = append(node.jobs, job)
}

func (node *Node) AddAlias(alias string, edge *Edge) {
	node.edgeAlias[alias] = edge
}

func (node *Node) AliasMap() map[string]*Edge {
	return node.edgeAlias
}

// Branch adds a 'BRANCH' transforms to the node and returns an edge connected
// from it.
func (node *Node) Branch(name, condition string) *Edge {
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

	node.setNodeType(TRANSFORM)
	node.SetName(fmt.Sprintf("Node%d", node.id))
	job := transforms.FilterFunction(filter)
	job.SetName(name)
	node.jobs = append(node.jobs, job)

	nt := &nodeTransform{node: node, job: job}

	e := newEdge(nt)
	node.edges = append(node.edges, e)

	return e
}

// Filter adds a 'FILTER' transforms to the node and returns an edge connected
// from it.
func (node *Node) Filter(name string, filter func(valMap map[string]interface{}) (bool, error)) *Edge {

	filt := func(m *message.Msg) (bool, error) {
		return filter(m.Values())
	}

	node.setNodeType(TRANSFORM)
	node.SetName(fmt.Sprintf("Node%d", node.id))
	job := transforms.FilterFunction(filt)
	job.SetName(name)
	node.jobs = append(node.jobs, job)

	nt := &nodeTransform{node: node, job: job}

	e := newEdge(nt)
	node.edges = append(node.edges, e)

	return e
}

// Join adds a 'JOIN' transforms to the node and returns an edge connected
// from it.
func (node *Node) Join(name string, event poll.Event, from []*Edge,
	desc []*table.Description, condition func(msg1, msg2 core.messageContent) bool) *Edge {

	if len(from) != 2 {
		log.Fatalf("Need two from edges, got= %v", len(from))
	}

	fromId := []uint{from[0].From().Id(), from[1].From().Id()}

	node.setNodeType(TRANSFORM)
	node.SetName(fmt.Sprintf("Node%d", node.id))

	join := transforms.NewJoin(event, desc, fromId, condition)
	job := join.Function()
	job.SetName(name)

	node.jobs = append(node.jobs, job)

	nt := &nodeTransform{node: node, job: job}

	e := newEdge(nt)
	node.edges = append(node.edges, e)

	join.Start()

	return e
}

// Lua adds a 'LUA' transforms to the node and returns an edge connected from it.
func (node *Node) Lua(name string, src []byte) *Edge {
	node.setNodeType(TRANSFORM)
	node.SetName(fmt.Sprintf("Node%d", node.id))

	lua := transforms.NewGopherLua(src)
	job := lua.GopherLuaFunction()
	job.SetName(name)
	node.jobs = append(node.jobs, job)

	nt := &nodeTransform{node: node, job: job}

	e := newEdge(nt)
	node.edges = append(node.edges, e)

	return e
}

func (node *Node) Aggregator(name string, event poll.Event, aggs []agg.Aggregator, groupBy ...string) *Edge {
	node.setNodeType(TRANSFORM)
	node.SetName(fmt.Sprintf("Node%d", node.id))

	aggregator := transforms.NewAggregator(event, aggs, groupBy...)
	job := aggregator.Function()
	job.SetName(name)

	node.jobs = append(node.jobs, job)

	nt := &nodeTransform{node: node, job: job}

	e := newEdge(nt)
	node.edges = append(node.edges, e)

	aggregator.Start()

	return e
}

// id returns the id of the node.
func (node *Node) Id() uint {
	return node.id
}

// Name returns the name of the node.
func (node *Node) Name() string {
	return node.name
}

// SetName gives a name to the node.
func (node *Node) SetName(name string) {
	node.name = name
}

// Jobs returns a slice of the jobs held by the node.
func (node *Node) Jobs() []pipeline.Executor {
	return node.jobs
}

// Edges returns a slice of the edges which starts from the node.
func (node *Node) Edges() []*Edge {
	return node.edges
}
