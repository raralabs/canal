package functions

import (
	"github.com/raralabs/canal/core/message"
	"github.com/raralabs/canal/core/transforms/agg"
	"github.com/raralabs/canal/utils/cast"
)

type Min struct {
	minVal *message.MsgFieldValue
	tmpl   agg.IAggFuncTemplate
}

func NewMin(tmpl agg.IAggFuncTemplate) *Min {
	return &Min{
		tmpl:   tmpl,
		minVal: message.NewFieldValue(nil, message.NONE),
	}
}

func (c *Min) Add(content *message.OrderedContent) {
	if c.tmpl.Filter(content.Values()) {
		val, ok := content.Get(c.tmpl.Field())
		if !ok {
			return
		}

		if c.minVal.Value() == nil {
			c.minVal.Val = val.Value()
			c.minVal.ValType = val.ValueType()
			return
		}

		switch val.ValueType() {
		case message.INT:
			m, _ := cast.TryInt(val.Value())
			cmp, _ := cast.TryInt(c.minVal.Value())

			mn := mini(m, cmp)
			c.minVal.Val = mn

		case message.FLOAT:
			m, _ := cast.TryFloat(val.Value())
			cmp, _ := cast.TryFloat(c.minVal.Value())

			mn := minf(m, cmp)
			c.minVal.Val = mn
		}
	}
}

func (c *Min) Result() *message.MsgFieldValue {
	return message.NewFieldValue(c.minVal.Value(), c.minVal.ValueType())
}

func (c *Min) Name() string {
	return c.tmpl.Name()
}

func (c *Min) Reset() {
	c.minVal.Val = nil
	c.minVal.ValType = message.NONE
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