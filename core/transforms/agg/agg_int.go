package agg

import (
	"github.com/raralabs/canal/core/message/content"
)

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
	Remove(prevContent content.IContent)

	// Add adds a message content to the aggregator.
	Add(value content.IContent)

	// Result returns result of the agg func.
	Result() *content.MsgFieldValue

	// Name returns the name of the agg func.
	Name() string

	// Reset resets the aggregator function.
	Reset()
}
