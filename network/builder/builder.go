package builder

import (
	"fmt"
	"log"

	"github.com/n-is/canal/sinks"

	"github.com/n-is/canal/network/builder/grammar/job/filter"
	"github.com/n-is/canal/network/builder/utils"

	"github.com/n-is/canal/network"
	"github.com/n-is/canal/network/builder/grammar/node"
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
	infos, err := node.Parse("", []byte(str))

	if err != nil {
		log.Fatalf("Error: %v\n", err)
	}

	parsed := utils.ToIfaceSlice(infos)

	// Each idiom should hold at least the name of the node and the job that
	// the idiom should create.
	// If the idiom is a node creation idiom, it will at least hold the name
	// of the node to be created, and the jobs from which the nodes should
	// receive streams of messages from.
	if len(parsed) >= 2 {
		nodeName := parsed[0]

		if n, ok := nodeName.(string); ok {

			description := parsed[1]

			// Handle Node Ceation
			var currNode *network.Node
			if len(b.Net.Nodes) == 0 {

				// Handle Genesis Node Creation
				currNode = b.Net.GenesisNode()
				b.nodes[n] = currNode

			} else if b.nodes[n] == nil {

				// Handle Node Creation from other nodes
				froms := description.(node.Creation)
				fromEdges := []*network.Edge{}
				for _, from := range froms.Incomings {
					nodeJob := utils.ToIfaceSlice(from)
					if len(nodeJob) != 2 {
						log.Fatalf("Error Getting from nodeJobs: %v\n", nodeJob)
					}
					if jobName, ok := nodeJob[1].(string); ok {
						nName, _ := nodeJob[0].(string)
						jn := fmt.Sprintf("%s%s", nName, jobName)
						fromEdges = append(fromEdges, b.jobs[jn])
					}
				}
				if len(fromEdges) == 0 {
					log.Fatalln("A node should get data from at least one job")
				}
				currNode = b.Net.NodeFrom(fromEdges...)
				b.nodes[n] = currNode

				return
			}

			// If the code reach here, it means the parsed value is an Instruction
			jobDesc := description.(node.Instruction)
			jobName := jobDesc.Name
			jobType := jobDesc.Type
			// jobGroup := jobDesc.Group
			// jobParams := jobDesc.Params

			// Check if the jobName already exists
			jn := fmt.Sprintf("%s%s", n, jobName)
			for k := range b.jobs {
				if k == jn {
					log.Fatalln("Need to provide separate job names for each job in a node")
				}
			}

			var edge *network.Edge

			// Create edge according to the job type provided.
			switch jobType {
			// Handle processors
			case utils.FILTER:
				filterDesc := jobDesc.Desc
				filt, err := filter.Parse("", []byte(filterDesc))

				if err != nil {
					log.Fatalf("Could not parse filter: %v\n", err)
				}

				if f, ok := filt.(filter.Filter); ok {
					edge = b.nodes[n].Filter(jobName, f)
				}

			case utils.BRANCH:
				branchDesc := jobDesc.Desc
				edge = b.nodes[n].Branch(jobName, branchDesc)

			case utils.COUNT:
				// counterDesc := jobDesc.Desc
				// counter, err := count.Parse("", []byte(counterDesc))

				// if err != nil {
				// 	log.Fatalf("Could not parse counter: %v\n", err)
				// }

				// if cnt, ok := counter.(poll.Event); ok {
				// 	edge = b.nodes[n].Count(jobName, cnt, jobParams, jobGroup...)
				// }

			case utils.LUA:

			case utils.PASS:

			// Handle Sinks
			case utils.STDOUT:
				job := sinks.NewStdoutSink()
				job.SetName(jobName)
				b.nodes[n].Sink(job)

			case utils.BLACKHOLE:
				job := sinks.NewBlackholeSink()
				job.SetName(jobName)
				b.nodes[n].Sink(job)
			}

			if edge != nil {
				b.jobs[jn] = edge
			}

			// Overwrite the node name
			b.nodes[n].SetName(n)
		}
	}
}
