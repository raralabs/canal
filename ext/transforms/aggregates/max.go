package aggregates

import (
	"github.com/raralabs/canal/core/message"
	"github.com/raralabs/canal/utils/cast"
)

// Max finds the max value in a stream if some conditions are satisfied
type Max struct {
	alias string                            // Alias of the max
	filt  func(map[string]interface{}) bool // The filter function
	field string                            // 'field' contains data whose maximum value is to be calculated
}

// NewMax creates a Max with the provided condition and returns it.
func NewMax(alias, field string, f func(map[string]interface{}) bool) *Max {
	if alias == "" {
		alias = "Max"
	}
	return &Max{alias: alias, field: field, filt: f}
}

// Name returns the name of the Max
func (c *Max) Name() string {
	return c.alias
}

// SetName sets the name of the Max
func (c *Max) SetName(alias string) {
	c.alias = alias
}

// Aggregate finds the maximum value based on the current value and the current
// message
func (c *Max) Aggregate(currentValue *message.MsgFieldValue, msg *message.OrderedContent) *message.MsgFieldValue {
	if c.filt != nil && currentValue == nil {
		if !c.filt(msg.Values()) {
			return nil
		}
	}

	content := *msg
	if _, ok := content.Get(c.field); !ok {
		return currentValue
	}

	if currentValue == nil {
		val, _ := content.Get(c.field)
		return val
	}

	v := currentValue.Value()

	switch currentValue.ValueType() {
	case message.INT:
		cmp, _ := cast.TryInt(v)
		val, _ := content.Get(c.field)
		m, _ := cast.TryInt(val.Value())

		mx := maxi(cmp, m)

		return message.NewFieldValue(mx, message.INT)

	case message.FLOAT:
		cmp, _ := cast.TryFloat(v)
		val, _ := content.Get(c.field)
		m, _ := cast.TryFloat(val.Value())

		mx := maxf(cmp, m)

		return message.NewFieldValue(mx, message.FLOAT)

	}
	return currentValue
}

// InitValue gives the initialization value for the max
func (c *Max) InitValue() *message.MsgFieldValue {

	return nil
}

// InitMsgValue gives the initialization value for the max based
// on the message
func (c *Max) InitMsgValue(msg *message.OrderedContent) *message.MsgFieldValue {

	if c.filt != nil {
		if !c.filt(msg.Values()) {
			return c.InitValue()
		}
	}
	m := *msg
	if v, ok := m.Get(c.field); ok {
		return v
	}
	return nil
}

func (c *Max) Reset() {

}

func maxi(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func maxf(a, b float64) float64 {
	if a > b {
		return a
	}
	return b
}
