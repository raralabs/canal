package agg

import (
	"github.com/raralabs/canal/core/message"
)

// An IAggregator aggregates data on the basis of the provided fileds
type IAggregator interface {
	// Name returns the name of the IAggregator
	Name() string

	// SetName sets the name of the IAggregator
	SetName(string)

	// Aggregate aggregates the data based on the current value and the current
	// message
	Aggregate(currentValue *message.MsgFieldValue, msg *message.MsgContent) *message.MsgFieldValue

	// InitValue gives the initialization value for the aggregator
	InitValue() *message.MsgFieldValue

	// InitMsgValue gives the initialization value for the aggregator based
	// on the message
	InitMsgValue(msg *message.MsgContent) *message.MsgFieldValue

	// Reset resets the aggregator functions' inner states
	Reset()
}
