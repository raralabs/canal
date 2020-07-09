package agg

import "github.com/raralabs/canal/core/message"

type IAggFuncTemplate interface {
	// Name returns the name of the aggregator template.
	Name() string

	// Field returns the field of the aggregator function.
	// For agg funcs like count, "" is returned.
	Field() string

	// Filter checks for the aggregator's inner condition.
	Filter(map[string]interface{}) bool

	// Function returns the aggregator function associated with
	// the template.
	Function() IAggFunc
}

type IAggFunc interface {
	// Add adds a message content to the aggregator.
	Add(value *message.OrderedContent)

	// Result returns result of the agg func.
	Result() *message.MsgFieldValue

	// Name returns the name of the agg func.
	Name() string

	// Reset resets the aggregator function.
	Reset()
}
