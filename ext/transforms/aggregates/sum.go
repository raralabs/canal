package aggregates

import (
	"github.com/raralabs/canal/core/message/content"
	"github.com/raralabs/canal/core/transforms/agg"
	"github.com/raralabs/canal/utils/cast"
)

func NewSum(alias, field string, filter func(map[string]interface{}) bool) *AggTemplate {
	if alias == "" {
		alias = "sum"
	}

	ag := NewAggTemplate(alias, field, filter)

	ag.function = func() agg.IAggFunc { return newSumFunc(ag) }

	return ag
}


type sum struct {
	tmpl agg.IAggFuncTemplate

	lastSum content.MsgFieldValue
}

func newSumFunc(tmpl agg.IAggFuncTemplate) *sum {
	return &sum{
		tmpl:    tmpl,
		lastSum: content.NewFieldValue(nil, content.NONE),
	}
}

func (c *sum) Remove(prevContent content.IContent) {

	if prevContent != nil {
		if old, ok := prevContent.Get(c.tmpl.Field()); ok {
			oldVal, _ := cast.TryFloat(old.Value())
			v1, _ := cast.TryFloat(c.lastSum.Value())
			c.lastSum.Val = v1 - oldVal
		}
	}
}

func (c *sum) Add(cntnt content.IContent) {
	if c.tmpl.Filter(cntnt.Values()) {
		val, ok := cntnt.Get(c.tmpl.Field())
		if !ok {
			return
		}

		if c.lastSum.Value() == nil {
			c.lastSum.Val = val.Value()
			c.lastSum.ValType = content.FLOAT
			return
		}

		switch val.ValueType() {
		case content.INT, content.FLOAT:
			v1, _ := cast.TryFloat(c.lastSum.Value())
			v2, _ := cast.TryFloat(val.Value())

			sum := v1 + v2

			c.lastSum.Val = sum
		}
	}
}

func (c *sum) Result() content.MsgFieldValue {
	return content.NewFieldValue(c.lastSum.Value(), c.lastSum.ValueType())
}

func (c *sum) Name() string {
	return c.tmpl.Name()
}

func (c *sum) Reset() {
	c.lastSum.Val = nil
	c.lastSum.ValType = content.NONE
}
