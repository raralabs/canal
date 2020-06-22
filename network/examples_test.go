package network

import (
	"github.com/raralabs/canal/sinks"
	"github.com/raralabs/canal/sources"
)

func ExampleNewNetwork() {

	networkId := uint(1)
	net := NewNetwork(networkId)

	less3 := Source(sources.NewInlineRange(int64(3)))

	genesisNode := net.GenesisNode()
	branch1 := genesisNode.Branch("Branch1", "value%2 == 0")
	branch2 := genesisNode.Branch("Branch2", "value%3 == 0")

	node2 := net.NodeFrom(branch1)
	branch3 := node2.Branch("Branch3", "value > 5")

	node3 := net.NodeFrom(branch2)
	branch4 := node3.Branch("Branch4", "value < 5")

	net.Sink(sinks.NewStdoutSink(), branch3, branch4)

	net.Build()
	net.Run(less3)

	// Output: [StdoutSink] map[value:3]
}
