package aggregates

import (
	"github.com/raralabs/canal/core/message"
	"github.com/raralabs/canal/utils/cast"
)

// Min finds the minimum value in a stream if some conditions are satisfied
type Min struct {
	alias string                            // Alias of the Min
	filt  func(map[string]interface{}) bool // The filter function
	field string                            // 'field' contains data whose min value is to be calculated
}

// NewMin creates a Min with the provided condition and returns it.
func NewMin(alias, field string, f func(map[string]interface{}) bool) *Min {
	return &Min{alias: alias, field: field, filt: f}
}

// Name returns the name of the Min
func (c *Min) Name() string {
	return c.alias
}

// SetName sets the name of the Min
func (c *Min) SetName(alias string) {
	c.alias = alias
}

// Aggregate finds the minimum value based on the current value and the current
// message
func (c *Min) Aggregate(currentValue *message.MsgFieldValue, msg *message.MsgContent) *message.MsgFieldValue {

	if c.filt != nil && currentValue == nil {
		if !c.filt(msg.Values()) {
			return nil
		}
	}

	content := *msg
	if currentValue == nil {
		return content[c.field]
	}

	v := currentValue.Value()

	switch currentValue.ValueType() {
	case message.INT:
		cmp, _ := cast.TryInt(v)
		m, _ := cast.TryInt(content[c.field].Value())

		mn := mini(cmp, m)

		return message.NewFieldValue(mn, message.INT)

	case message.FLOAT:
		cmp, _ := cast.TryFloat(v)
		m, _ := cast.TryFloat(content[c.field].Value())

		mn := minf(cmp, m)

		return message.NewFieldValue(mn, message.FLOAT)

	}
	return currentValue
}

// InitValue gives the initialization value for the minimum
func (c *Min) InitValue() *message.MsgFieldValue {

	return nil
}

// InitMsgValue gives the initialization value for the Min based
// on the message
func (c *Min) InitMsgValue(msg *message.MsgContent) *message.MsgFieldValue {

	if c.filt != nil {
		if !c.filt(msg.Values()) {
			return c.InitValue()
		}
	}
	m := *msg
	return m[c.field]
}

func (c *Min) Reset() {

}

func mini(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func minf(a, b float64) float64 {
	if a < b {
		return a
	}
	return b
}