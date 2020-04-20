package main

import (
	"github.com/raralabs/canal/network"
	"github.com/raralabs/canal/sinks"
	"github.com/raralabs/canal/sources"
)

func main() {
	net := network.NewNetwork(1)

	root := net.GenesisNode()
	even := root.Branch("even", "value % 2 == 0")
	divBy3 := root.Branch("div_by_4", "value % 4 == 0")
	mergedNode := net.NodeFrom(even, divBy3)
	mergedNode.Filter("filtered", func(valMap map[string]interface{}) (bool, error) {
		return true, nil
	})

	net.Sink(sinks.NewStdoutSink(), mergedNode.Edges()...)
	net.Build()

	source := network.Source(sources.NewInlineRange(10))

	net.Run(source)
}
