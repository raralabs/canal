package network

import (
	"io/ioutil"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/raralabs/canal/sinks"
	"github.com/raralabs/canal/sources"
)

func TestNetwork(t *testing.T) {

	networkId := uint(1)
	var net *Network
	t.Run("Testing NewNetwork", func(t *testing.T) {
		assert.Nil(t, net, "Network not yet created")
		net = NewNetwork(networkId)
		assert.NotNil(t, net, "Network should have been created")
	})

	var srcs []*Node
	t.Run("Testing Source stages Creation", func(t *testing.T) {
		less3 := Source(sources.NewInlineRange(int64(3)))
		srcs = append(srcs, less3)

		assert.Equal(t, 1, len(srcs), "Only one source created")

		for _, src := range srcs {
			assert.Zero(t, src.Id(), "id of source should always be 0")
			assert.Equal(t, 1, len(src.Jobs()), "Each source should hold only one job")
		}
	})

	var genesisNode *Node
	t.Run("Testing Genesis Node Creation", func(t *testing.T) {
		genesisNode = net.GenesisNode()
		assert.Equal(t, uint(1), genesisNode.Id(), "id of genesis node should always be 1")

		anotherGenesis := func() {
			net.GenesisNode()
		}

		assert.Panics(t, anotherGenesis, "Should panic if multiple genesis nodes are created in a network")
	})

	var edgesFromGenesis []*Edge
	t.Run("Testing Edges Creation", func(t *testing.T) {
		branch1 := genesisNode.Branch("Branch1", "value%2 == 0")
		branch2 := genesisNode.Branch("Branch2", "value%3 == 0")

		filter := genesisNode.Filter("Filter", func(valMap map[string]interface{}) (bool, error) {
			return valMap["value"] != 1, nil
		})

		lua := genesisNode.Lua("Lua", []byte(`
		function GopherLua(m)
			return m
		end`))

		assert.Nil(t, branch1.To(), "The edge have not been terminated")
		assert.Nil(t, branch2.To(), "The edge have not been terminated")
		assert.Nil(t, filter.To(), "The edge have not been terminated")
		assert.Nil(t, lua.To(), "The edge have not been terminated")
		assert.Equal(t, "Branch1", branch1.FromJob().Name(), "Edge From Branch NodeJob")
		assert.Equal(t, "Branch2", branch2.FromJob().Name(), "Edge From Branch NodeJob")
		assert.Equal(t, "Filter", filter.FromJob().Name(), "Edge From Branch NodeJob")
		assert.Equal(t, "Lua", lua.FromJob().Name(), "Edge From Branch NodeJob")
		assert.Equal(t, "Node1", branch1.From().Name(), "Edge From First Node")

		edgesFromGenesis = append(edgesFromGenesis, branch1, branch2)

		net.Sink(sinks.NewBlackholeSink(), filter, lua)
	})

	var toSinkEdges []*Edge
	t.Run("Testing stages Creation From Edges", func(t *testing.T) {
		node2 := net.NodeFrom(edgesFromGenesis[0])
		branch3 := node2.Branch("Branch3", "value > 5")

		node3 := net.NodeFrom(edgesFromGenesis[1])
		branch4 := node3.Branch("Branch4", "value < 5")

		assert.Nil(t, branch3.To(), "The edge have not been terminated")
		assert.Nil(t, branch4.To(), "The edge have not been terminated")
		assert.NotNil(t, edgesFromGenesis[0].To(), "The edge have been terminated")
		assert.NotNil(t, edgesFromGenesis[1].To(), "The edge have been terminated")

		toSinkEdges = append(toSinkEdges, branch3, branch4)
	})

	t.Run("Testing Network Sinks", func(t *testing.T) {
		numNodes := len(net.Nodes)
		net.Sink(sinks.NewStdoutSink(), toSinkEdges...)

		assert.Equal(t, numNodes+1, len(net.Nodes), "Sink is also just a node")
	})

	t.Run("Testing Network Build", func(t *testing.T) {
		runBeforeBuild := func() {
			net.Run(srcs...)
		}
		assert.Panics(t, runBeforeBuild, "Should Panic if network is run before it's built")

		assert.Nil(t, net.pipeline, "The backend network is not created before network build")
		net.Build()
		assert.NotNil(t, net.pipeline, "Backend network have been created")

		assert.Equal(t, 5, len(net.pipeline.stages), "Five Hubs have been created")
	})

	t.Run("Testing Network Run", func(t *testing.T) {
		file, err := ioutil.TempFile("", "net")
		if err != nil {
			t.Fatalf("File Creation Failed: %v", err)
		}
		defer os.Remove(file.Name())

		sout := os.Stdout
		os.Stdout = file
		net.Run(srcs...)
		os.Stdout = sout

		name := file.Name()
		file.Close()

		output, err := ioutil.ReadFile(name)
		if err != nil {
			t.Fatalf("File Opening Failed: %v", err)
		}

		outStr := strings.TrimSpace(string(output))

		assert.Equal(t, "[StdoutSink] map[value:3]", outStr, "Output should be map with value 3")
	})
}
