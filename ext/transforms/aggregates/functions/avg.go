package functions

import (
	"sync/atomic"

	"github.com/raralabs/canal/core/message"
	"github.com/raralabs/canal/core/transforms/agg"
	"github.com/raralabs/canal/utils/cast"
)

type Avg struct {
	tmpl agg.IAggFuncTemplate

	lastAvg *message.MsgFieldValue
	num     uint64
}

func NewAvg(tmpl agg.IAggFuncTemplate) *Avg {
	return &Avg{
		tmpl:    tmpl,
		lastAvg: message.NewFieldValue(nil, message.NONE),
	}
}

func (c *Avg) Add(content, prevContent *message.OrderedContent) {
	if c.tmpl.Filter(content.Values()) {
		val, ok := content.Get(c.tmpl.Field())
		if !ok {
			return
		}

		if c.lastAvg.Value() == nil {
			c.lastAvg.Val = val.Value()
			c.lastAvg.ValType = message.FLOAT
			atomic.AddUint64(&c.num, 1)
			return
		}

		switch val.ValueType() {
		case message.INT, message.FLOAT:
			v1, _ := cast.TryFloat(c.lastAvg.Value())
			v2, _ := cast.TryFloat(val.Value())

			mean := v1*float64(c.num) + v2
			atomic.AddUint64(&c.num, 1)
			mean /= float64(c.num)

			c.lastAvg.Val = mean
		}
	}
}

func (c *Avg) Result() *message.MsgFieldValue {
	return message.NewFieldValue(c.lastAvg.Value(), c.lastAvg.ValueType())
}

func (c *Avg) Name() string {
	return c.tmpl.Name()
}

func (c *Avg) Reset() {
	c.num = 0
	c.lastAvg.Val = nil
	c.lastAvg.ValType = message.NONE
}
