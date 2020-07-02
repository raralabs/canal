package aggregates

import (
	"log"

	"github.com/raralabs/canal/core/message"
	"github.com/raralabs/canal/utils/cast"
)

// Variance finds the average value in a stream if some conditions are satisfied
type Variance struct {
	alias string                            // Alias of the Variance
	filt  func(map[string]interface{}) bool // The filter function
	field string                            // 'field' contains data whose variance is to be calculated

	num      uint64  // The number of elements that has arrived
	lastMean float64 // Mean of the last data
	lastV    float64 // The v
}

// NewVariance creates a Variance with the provided condition and returns it.
func NewVariance(alias, field string, f func(map[string]interface{}) bool) *Variance {
	return &Variance{alias: alias, field: field,
		filt: f, num: uint64(0), lastMean: float64(0), lastV: float64(0)}
}

// Name returns the name of the Variance
func (c *Variance) Name() string {
	return c.alias
}

// SetName sets the name of the Variance
func (c *Variance) SetName(alias string) {
	c.alias = alias
}

// Aggregate calculates variance of the data based on the current value and the current
// message
func (c *Variance) Aggregate(currentValue *message.MsgFieldValue, msg *message.MsgContent) *message.MsgFieldValue {

	if c.num == MaxUint64 {
		log.Panicln("Maximum possible number of elements have been passed")
		return currentValue
	} else if c.num == 0 {
		return c.InitMsgValue(msg)
	}

	if c.filt != nil {
		if !c.filt(msg.Values()) {
			return currentValue
		}
	}

	switch currentValue.ValueType() {
	case message.INT, message.FLOAT:
		lastV := c.lastV
		content := *msg
		xk, _ := cast.TryFloat(content[c.field].Value())

		c.num += 1
		newMean := c.lastMean + (xk-c.lastMean)/float64(c.num)

		vk := lastV + (xk-c.lastMean)*(xk-newMean)
		variance := vk / float64(c.num-1)

		c.lastMean = newMean
		c.lastV = vk

		return message.NewFieldValue(variance, message.FLOAT)
	}
	return currentValue
}

// InitValue gives the initialization value for the Variance
func (c *Variance) InitValue() *message.MsgFieldValue {

	return message.NewFieldValue(int64(0), message.INT)
}

// InitMsgValue gives the initialization value for the Variance based
// on the message
func (c *Variance) InitMsgValue(msg *message.MsgContent) *message.MsgFieldValue {

	if c.filt != nil {
		if !c.filt(msg.Values()) {
			return c.InitValue()
		}
	}
	c.num += 1
	content := *msg
	c.lastMean, _ = cast.TryFloat(content[c.field].Value())
	return message.NewFieldValue(float64(0), message.FLOAT)
}

func (c *Variance) Reset() {
	c.num = uint64(0)
	c.lastMean = 0
	c.lastV = 0
}
