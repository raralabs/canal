package builder

import (
	"fmt"
	"github.com/n-is/canal/core/pipeline"
	"log"

	"github.com/n-is/canal/core"
	"github.com/n-is/canal/network"
	"github.com/n-is/canal/network/builder/grammar2/idiom"
	"github.com/n-is/canal/sinks"
	"github.com/n-is/canal/transforms/agg"
	"github.com/n-is/canal/transforms/join/table"
)

// A Builder builds a network, on the basis of our DSL(Domain Specific Language).
type Builder struct {
	Net   *network.Network
	jobs  map[string]*network.Edge
	nodes map[string]*network.Node
}

// BuildNetwork creates a builder that builds a network with the given id, and
// returns it.
func BuildNetwork(id uint) *Builder {
	return &Builder{Net: network.NewNetwork(id),
		jobs:  make(map[string]*network.Edge),
		nodes: make(map[string]*network.Node),
	}
}

// Build builds the network that the builder creates.
func (b *Builder) Build() {
	b.Net.Build()
}

// Run runs the network that the builder creates.
func (b *Builder) Run(srcs ...*network.Node) {
	b.Net.Run(srcs...)
}

// add adds a job to the node in the network. It accepts the idiom in our DSL.
// If the node doesn't exist in the network, the node is created.
func (b *Builder) Add(str string) {

	infos, err := idiom.Parse("", []byte(str))

	if err != nil {
		log.Fatalf("Error: %v\n", err)
	}

	var currNode *network.Node

	switch info := infos.(type) {

	case idiom.NodeCreator:
		n := info.Name
		if len(b.Net.Nodes) == 0 {
			if len(info.Froms) == 1 {
				if info.Froms[0].Genesis {
					// Handle Genesis Node Creation
					currNode = b.Net.GenesisNode()
					b.nodes[n] = currNode
				}
			}
		} else if b.nodes[n] == nil {

			// Handle Node Creation from other nodes
			froms := info.Froms
			fromEdges := []*network.Edge{}

			aliasMap := make(map[string]*network.Edge)

			for _, from := range froms {
				nName := from.Node
				jobName := from.Job
				jn := fmt.Sprintf("%s%s", nName, jobName)
				fromEdges = append(fromEdges, b.jobs[jn])

				if from.Alias != "" {
					aliasMap[from.Alias] = b.jobs[jn]
				}
			}
			if len(fromEdges) == 0 {
				log.Fatalln("A node should get data from at least one job")
			}
			currNode = b.Net.NodeFrom(fromEdges...)
			b.nodes[n] = currNode

			for k, v := range aliasMap {
				b.nodes[n].AddAlias(k, v)
			}

			return
		}

	case idiom.NodeJob:
		n := info.NodeName
		jName := info.JobName

		// Check if the jobName already exists
		jn := fmt.Sprintf("%s%s", n, jName)
		for k := range b.jobs {
			if k == jn {
				log.Fatalln("Need to provide separate job names for each job in a node")
			}
		}

		var edge *network.Edge

		jobParam := info.Params
		switch job := jobParam.(type) {

		case idiom.DoNodeJob:
			f := job.Function
			switch doJob := f.(type) {
			case idiom.Filter:
				edge = b.nodes[n].Filter(jName, doJob)
			case idiom.Branch:
				edge = b.nodes[n].Branch(jName, doJob.Condition)
			}

		case idiom.AggNodeJob:
			trg := job.Trigger
			if trg == nil {
				log.Fatalf("Need Trigger Condition for %s %s", n, jName)
			}
			var aggs []agg.Aggregator
			for _, jb := range job.Functions {
				switch ag := jb.(type) {

				case idiom.Count:
					alias := ag.Alias
					filt := func(m map[string]interface{}) bool {
						out, err := ag.Filter(m)
						if err != nil {
							log.Printf("Error in count filter: %v", err)
						}
						return out
					}
					counter := agg.NewCount(alias, filt)
					aggs = append(aggs, counter)

				case idiom.Max:
					alias := ag.Alias
					filt := func(m map[string]interface{}) bool {
						out, err := ag.Filter(m)
						if err != nil {
							log.Printf("Error in count filter: %v", err)
						}
						return out
					}
					max := agg.NewMax(alias, ag.Field, filt)
					aggs = append(aggs, max)

				case idiom.Min:
					alias := ag.Alias
					filt := func(m map[string]interface{}) bool {
						out, err := ag.Filter(m)
						if err != nil {
							log.Printf("Error in count filter: %v", err)
						}
						return out
					}
					min := agg.NewMin(alias, ag.Field, filt)
					aggs = append(aggs, min)

				}
			}

			groupBy := job.GroupBy
			edge = b.nodes[n].Aggregator(jName, trg, aggs, groupBy...)

		case idiom.JoinNodeJob:
			s1 := job.Streams[0]
			s2 := job.Streams[1]

			trg := job.Trigger
			if trg == nil {
				log.Fatalf("Need Trigger Condition for %s %s", n, jName)
			}

			jnFilter := job.Filter
			var s1Flds, s2Flds []string

			for _, jnVals := range jnFilter.JoinValues {
				if jnVals.Stream == s1 {
					s1Flds = appendUniqueStr(s1Flds, jnVals.Field)
				} else if jnVals.Stream == s2 {
					s2Flds = appendUniqueStr(s2Flds, jnVals.Field)
				} else {
					log.Fatalf("Unknown stream: %v", jnVals.Stream)
				}
			}

			condition := func(m1, m2 core.messageContent) bool {

				values := make(map[string]interface{})

				m1Vals := m1.Values()
				for _, flds := range s1Flds {
					key := fmt.Sprintf("%s%s", s1, flds)
					if v, ok := m1Vals[flds]; ok {
						values[key] = v
					} else {
						return false
					}
				}

				m2Vals := m2.Values()
				for _, flds := range s2Flds {
					key := fmt.Sprintf("%s%s", s2, flds)
					if v, ok := m2Vals[flds]; ok {
						values[key] = v
					} else {
						return false
					}
				}

				out, _ := jnFilter.Filter(values)

				return out
			}

			aliasMap := b.nodes[n].AliasMap()
			from := []*network.Edge{
				aliasMap[s1],
				aliasMap[s2],
			}

			simpleCtx := table.NewSimpleContext(true, []string{}, nil, nil)
			desc := []*table.Description{
				&table.Description{Alias: s1, Fields: s1Flds, Ctx: simpleCtx, Type: "mem"},
				&table.Description{Alias: s2, Fields: s2Flds, Ctx: simpleCtx, Type: "mem"},
				&table.Description{Alias: "Joined", Fields: []string{}, Ctx: simpleCtx, Type: "mem"},
			}

			edge = b.nodes[n].Join(jName, trg, from, desc, condition)

		case idiom.SinkJob:
			sink := job.Type
			var jb pipeline.Executor
			if sink == "stdout" {
				jb = sinks.NewStdoutSink()

			} else if sink == "blackhole" {
				jb = sinks.NewBlackholeSink()
			}

			if jb != nil {
				jb.SetName(jName)
				b.nodes[n].Sink(jb)
			}
		}

		if edge != nil {
			b.jobs[jn] = edge
		}

		// Overwrite the node name
		b.nodes[n].SetName(n)
	}

	_ = infos
}

func appendUniqueStr(strs []string, str string) []string {
	for _, v := range strs {
		if str == v {
			return strs
		}
	}
	strs = append(strs, str)
	return strs
}
