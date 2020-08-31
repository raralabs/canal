package aggregates

import (
	"github.com/raralabs/canal/core/message/content"
	"github.com/raralabs/canal/core/transforms/agg"
	"github.com/raralabs/canal/utils/cast"
	stream_math "github.com/raralabs/canal/utils/stream-math"
)

func NewPValue(alias, field1, field2 string, filter func(map[string]interface{}) bool) *AggTemplate {
	if alias == "" {
		alias = "pvalue"
	}

	ag := NewAggTemplate(alias, field1, filter)

	ag.function = func() agg.IAggFunc {
		return newPValueFunc(ag, func() string {
			return field2
		})
	}
	return ag
}

type pvalue struct {
	tmpl agg.IAggFuncTemplate

	pv     *stream_math.PValue
	field2 func() string
}

func newPValueFunc(tmpl agg.IAggFuncTemplate, field2 func() string) *pvalue {
	return &pvalue{
		tmpl:   tmpl,
		pv:     stream_math.NewPValue(),
		field2: field2,
	}
}

func (c *pvalue) Remove(prevContent content.IContent) {
	if prevContent != nil {
		vl1, ok1 := prevContent.Get(c.tmpl.Field())
		vl2, ok2 := prevContent.Get(c.field2())

		if ok1 && ok2 {
			x2, _ := cast.TryFloat(vl1.Val)
			y2, _ := cast.TryFloat(vl2.Val)

			c.pv.Remove(x2, y2)
		}
	}
}

func (c *pvalue) Add(cntnt content.IContent) {
	if c.tmpl.Filter(cntnt.Values()) {
		val1, ok := cntnt.Get(c.tmpl.Field())
		if !ok {
			return
		}
		val2, ok := cntnt.Get(c.field2())
		if !ok {
			return
		}

		switch val1.ValueType() {
		case content.INT, content.FLOAT:
			x1, _ := cast.TryFloat(val1.Val)
			y1, _ := cast.TryFloat(val2.Val)

			c.pv.Add(x1, y1)
		}
	}
}

func (c *pvalue) Result() content.MsgFieldValue {
	res, err := c.pv.Result()
	if err != nil {
		return content.NewFieldValue(nil, content.NONE)
	}
	return content.NewFieldValue(res, content.FLOAT)
}

func (c *pvalue) Name() string {
	return c.tmpl.Name()
}

func (c *pvalue) Reset() {
	c.pv.Reset()
}
