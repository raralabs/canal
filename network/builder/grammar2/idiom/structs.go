package idiom

import (
	"github.com/n-is/canal/transforms/event/poll"
)

type From struct {
	Node    string //
	Job     string //
	Alias   string //
	Genesis bool   // Denotes Genesis node if true
}

type NodeCreator struct {
	Name  string
	Froms []From
}

type NodeJob struct {
	NodeName string
	JobName  string
	Params   interface{}
}

type DoNodeJob struct {
	Function interface{}
}

type AggNodeJob struct {
	Trigger   poll.Event
	Functions []interface{}
	GroupBy   []string
}

type JoinNodeJob struct {
	Trigger poll.Event
	Streams []string
	Filter  JoinFilter
}

type JoinFilter struct {
	Filter     Filter
	JoinValues []JoinValue
}

type JoinValue struct {
	Stream string
	Field  string
}

type SinkJob struct {
	Type string
}

// Do Functions
type Filter func(map[string]interface{}) (bool, error)

type Branch struct {
	Condition string
}

// Aggregator Functions

type Count struct {
	Alias  string
	Filter Filter
}

type Max struct {
	Alias  string
	Field  string
	Filter Filter
}

type Min struct {
	Alias  string
	Field  string
	Filter Filter
}
