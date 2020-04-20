package network

import (
	"github.com/n-is/canal/sinks"

	"github.com/n-is/canal/transforms/agg"

	"github.com/n-is/canal/sources"
	"github.com/n-is/canal/transforms/event/poll"
)

func ExampleNode_Aggregator() {

	// Example for Count
	func() {
		networkId := uint(1)
		net := NewNetwork(networkId)

		values := []map[string]interface{}{
			{
				"name":  "Nepal",
				"value": 1,
				"greet": "Hello",
			},
			{
				"value": -1,
				"name":  "Nischal",
				"greet": "Nihao",
			},
		}

		randValues := Source(sources.NewMapValueSource(values, 2))

		// Events for the aggregator to emit messages
		filtEvent := poll.NewFilterEvent(func(m map[string]interface{}) bool {
			// cnt := m["Counter A"].(uint64)
			// return cnt%2 == 0
			return true
		})
		// timerEvent := poll.NewTimerEvent(1 * time.Millisecond)
		event := poll.NewCompositeEvent("and", filtEvent)

		// Aggregator Functions held by the aggregator job
		counterA := agg.NewCount("CounterA", func(m map[string]interface{}) bool {
			return m["name"] == "Nischal"
		})
		counterB := agg.NewCount("CounterB", func(m map[string]interface{}) bool {
			return m["name"] == "Nepal"
		})
		aggs := []agg.Aggregator{counterA, counterB}

		genesisNode := net.GenesisNode()
		branch1 := genesisNode.Aggregator("Counter", event, aggs, "name", "greet")

		net.Sink(sinks.NewStdoutSink(), branch1)

		net.Build()

		net.Run(randValues)

	}()

	// Example for Mean
	func() {
		networkId := uint(1)
		net := NewNetwork(networkId)

		values := []map[string]interface{}{
			{
				"name":  "Nepal",
				"value": 1,
				"greet": "Hello",
			},
			{
				"value": -1,
				"name":  "Nischal",
				"greet": "Nihao",
			},
		}

		randValues := Source(sources.NewMapValueSource(values, 2))

		// Events for the aggregator to emit messages
		filtEvent := poll.NewFilterEvent(func(m map[string]interface{}) bool {
			// cnt := m["Counter A"].(uint64)
			// return cnt%2 == 0
			return true
		})
		// timerEvent := poll.NewTimerEvent(1 * time.Millisecond)
		event := poll.NewCompositeEvent("and", filtEvent)

		// Aggregator Functions held by the aggregator job
		meanA := agg.NewMean("MeanA", "value", func(m map[string]interface{}) bool {
			return m["name"] == "Nischal"
		})
		meanB := agg.NewMean("MeanB", "value", func(m map[string]interface{}) bool {
			return m["name"] == "Nepal"
		})
		aggs := []agg.Aggregator{meanA, meanB}

		genesisNode := net.GenesisNode()
		branch1 := genesisNode.Aggregator("Mean", event, aggs, "name", "greet")

		net.Sink(sinks.NewStdoutSink(), branch1)

		net.Build()

		net.Run(randValues)
	}()

	// Unordered output:
	// [StdoutSink] map[CounterA:0 CounterB:1 greet:Hello name:Nepal]
	// [StdoutSink] map[CounterA:0 CounterB:1 greet:Hello name:Nepal]
	// [StdoutSink] map[CounterA:1 CounterB:0 greet:Nihao name:Nischal]
	// [StdoutSink] map[CounterA:0 CounterB:2 greet:Hello name:Nepal]
	// [StdoutSink] map[CounterA:1 CounterB:0 greet:Nihao name:Nischal]
	// [StdoutSink] map[CounterA:0 CounterB:2 greet:Hello name:Nepal]
	// [StdoutSink] map[CounterA:2 CounterB:0 greet:Nihao name:Nischal]
	// [StdoutSink] map[MeanA:0 MeanB:1 greet:Hello name:Nepal]
	// [StdoutSink] map[MeanA:0 MeanB:1 greet:Hello name:Nepal]
	// [StdoutSink] map[MeanA:-1 MeanB:0 greet:Nihao name:Nischal]
	// [StdoutSink] map[MeanA:0 MeanB:1 greet:Hello name:Nepal]
	// [StdoutSink] map[MeanA:-1 MeanB:0 greet:Nihao name:Nischal]
	// [StdoutSink] map[MeanA:0 MeanB:1 greet:Hello name:Nepal]
	// [StdoutSink] map[MeanA:-1 MeanB:0 greet:Nihao name:Nischal]
}
