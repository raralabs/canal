package aggregates

import (
	"github.com/raralabs/canal/core/message"
)

// Count is a counter that counts if some conditions are satisfied
type Count struct {
	alias string                            // Alias of the counter
	filt  func(map[string]interface{}) bool // The filter function
}

// NewCount creates a Count with the provided condition and returns it.
func NewCount(alias string, condition func(map[string]interface{}) bool) *Count {
	if alias == "" {
		alias = "Count"
	}
	return &Count{alias: alias, filt: condition}
}

// Name returns the name of the Counter
func (c *Count) Name() string {
	return c.alias
}

// SetName sets the name of the Counter
func (c *Count) SetName(alias string) {
	c.alias = alias
}

// Aggregate counts the data based on the current value and the current
// message
func (c *Count) Aggregate(currentValue *message.MsgFieldValue, msg *message.MsgContent) *message.MsgFieldValue {

	v := currentValue.Value()
	// Maybe we could assert that v's type is INT

	v1 := v.(uint64)
	if c.filt != nil {
		if !c.filt(msg.Values()) {
			return message.NewFieldValue(v1, message.INT)
		}
	}

	return message.NewFieldValue(v1+1, message.INT)
}

// InitValue gives the initialization value for the counter
func (c *Count) InitValue() *message.MsgFieldValue {

	return message.NewFieldValue(uint64(0), message.INT)
}

// InitMsgValue gives the initialization value for the counter based
// on the message
func (c *Count) InitMsgValue(msg *message.MsgContent) *message.MsgFieldValue {

	if c.filt != nil {
		if !c.filt(msg.Values()) {
			return c.InitValue()
		}
	}
	return message.NewFieldValue(uint64(1), message.INT)
}

func (c *Count) Reset() {

}
