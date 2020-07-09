package functions

import (
	"github.com/raralabs/canal/core/message"
	"github.com/raralabs/canal/core/transforms/agg"
	"github.com/raralabs/canal/utils/cast"
)

type Min struct {
	maxVal *message.MsgFieldValue
	tmpl agg.IAggFuncTemplate
}

func NewMin(tmpl agg.IAggFuncTemplate) *Min {
	return &Min{
		tmpl: tmpl,
		maxVal: message.NewFieldValue(nil, message.NONE),
	}
}

func (c *Min) Add(content *message.OrderedContent) {
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

			mn := mini(m, cmp)
			c.maxVal.Val = mn

		case message.FLOAT:
			m, _ := cast.TryFloat(val.Value())
			cmp, _ := cast.TryFloat(c.maxVal.Value())

			mn := minf(m, cmp)
			c.maxVal.Val = mn
		}
	}
}

func (c *Min) Result() *message.MsgFieldValue {
	return message.NewFieldValue(c.maxVal.Value(), c.maxVal.ValueType())
}

func (c *Min) Name() string {
	return c.tmpl.Name()
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