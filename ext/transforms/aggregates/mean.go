package aggregates

import (
	"log"

	"github.com/raralabs/canal/core/message"
	"github.com/raralabs/canal/utils/cast"
)

const MaxUint64 = ^uint64(0)

// Mean finds the average value in a stream if some conditions are satisfied
type Mean struct {
	alias string                            // Alias of the Mean
	filt  func(map[string]interface{}) bool // The filter function
	field string                            // 'field' contains data whose mean is to be calculated

	num uint64 // The number of elements that has arrived
}

// NewMean creates a Mean with the provided condition and returns it.
func NewMean(alias, field string, f func(map[string]interface{}) bool) *Mean {
	return &Mean{alias: alias, field: field, filt: f, num: uint64(0)}
}

// Name returns the name of the Mean
func (c *Mean) Name() string {
	return c.alias
}

// SetName sets the name of the Mean
func (c *Mean) SetName(alias string) {
	c.alias = alias
}

// Aggregate finds the mean value based on the current value and the current
// message
func (c *Mean) Aggregate(currentValue *message.MsgFieldValue, msg *message.MsgContent) *message.MsgFieldValue {

	if c.num == MaxUint64 {
		log.Panicln("Maximum possible number of elements have been passed")
		return currentValue
	} else if c.num == 0 {
		return c.InitMsgValue(msg)
	}

	v := currentValue.Value()

	if c.filt != nil {
		if !c.filt(msg.Values()) {
			return currentValue
		}
	}

	content := *msg
	switch currentValue.ValueType() {
	case message.INT, message.FLOAT:
		v1, _ := cast.TryFloat(v)
		v2, _ := cast.TryFloat(content[c.field].Value())

		mean := v1*float64(c.num) + v2
		c.num += 1
		mean /= float64(c.num)

		return message.NewFieldValue(mean, message.FLOAT)
	}
	return currentValue
}

// InitValue gives the initialization value for the Mean
func (c *Mean) InitValue() *message.MsgFieldValue {

	return message.NewFieldValue(int64(0), message.INT)
}

// InitMsgValue gives the initialization value for the Mean based
// on the message
func (c *Mean) InitMsgValue(msg *message.MsgContent) *message.MsgFieldValue {

	if c.filt != nil {
		if !c.filt(msg.Values()) {
			return c.InitValue()
		}
	}
	c.num += 1
	content := *msg
	return content[c.field]
}

func (c *Mean) Reset() {
	c.num = uint64(0)
}
