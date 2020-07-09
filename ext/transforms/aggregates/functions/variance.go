package functions

import (
	"github.com/raralabs/canal/core/message"
	"github.com/raralabs/canal/core/transforms/agg"
	"github.com/raralabs/canal/utils/cast"
	"sync/atomic"
)

type Variance struct {
	tmpl agg.IAggFuncTemplate

	num      uint64  // The number of elements that has arrived
	lastMean float64 // Mean of the last data
	lastV    float64 // The v
	variance *message.MsgFieldValue
}

func NewVariance(tmpl agg.IAggFuncTemplate) *Variance {
	return &Variance{
		tmpl: tmpl,
		variance: message.NewFieldValue(nil, message.NONE),
		num: uint64(0),
		lastMean: float64(0),
		lastV: float64(0),
	}
}

func (c *Variance) Add(content *message.OrderedContent) {
	if c.tmpl.Filter(content.Values()) {
		val, ok := content.Get(c.tmpl.Field())
		if !ok {
			return
		}

		if c.variance.Value() == nil {
			c.variance.Val = float64(0)
			c.variance.ValType = message.FLOAT

			atomic.AddUint64(&c.num, 1)
			c.lastMean, _ = cast.TryFloat(val.Value())
			return
		}

		switch val.ValueType() {
		case message.INT, message.FLOAT:
			lastV := c.lastV
			xk, _ := cast.TryFloat(val.Value())

			atomic.AddUint64(&c.num, 1)
			newMean := c.lastMean + (xk-c.lastMean)/float64(c.num)

			vk := lastV + (xk-c.lastMean)*(xk-newMean)
			variance := vk / float64(c.num-1)

			c.lastMean = newMean
			c.lastV = vk

			c.variance.Val = variance
		}
	}
}

func (c *Variance) Result() *message.MsgFieldValue {
	return message.NewFieldValue(c.variance.Value(), c.variance.ValueType())
}

func (c *Variance) Name() string {
	return c.tmpl.Name()
}

func (c *Variance) Reset() {
	c.variance.Val = nil
	c.variance.ValType = message.NONE

	c.num = 0
	c.lastMean = 0
	c.lastV = 0
}
