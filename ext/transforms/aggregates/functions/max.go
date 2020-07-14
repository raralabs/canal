package functions

import (
	"github.com/raralabs/canal/core/message"
	"github.com/raralabs/canal/core/transforms/agg"
	"github.com/raralabs/canal/utils/cast"
)

type Max struct {
	maxVal *message.MsgFieldValue
	tmpl   agg.IAggFuncTemplate
}

func NewMax(tmpl agg.IAggFuncTemplate) *Max {
	return &Max{
		tmpl:   tmpl,
		maxVal: message.NewFieldValue(nil, message.NONE),
	}
}

func (c *Max) Add(content, prevContent *message.OrderedContent) {
	if c.tmpl.Filter(content.Values()) {
		val, ok := content.Get(c.tmpl.Field())
		if !ok {
			return
		}

		if c.maxVal.Value() == nil {
			c.maxVal.Val = val.Value()
			c.maxVal.ValType = val.ValueType()
			return
		}

		switch val.ValueType() {
		case message.INT:
			m, _ := cast.TryInt(val.Value())
			cmp, _ := cast.TryInt(c.maxVal.Value())

			mx := maxi(m, cmp)
			c.maxVal.Val = mx

		case message.FLOAT:
			m, _ := cast.TryFloat(val.Value())
			cmp, _ := cast.TryFloat(c.maxVal.Value())

			mx := maxf(m, cmp)
			c.maxVal.Val = mx
		}
	}
}

func (c *Max) Result() *message.MsgFieldValue {
	return message.NewFieldValue(c.maxVal.Value(), c.maxVal.ValueType())
}

func (c *Max) Name() string {
	return c.tmpl.Name()
}

func (c *Max) Reset() {
	c.maxVal.Val = nil
	c.maxVal.ValType = message.NONE
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
