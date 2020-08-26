package aggregates

import (
	"github.com/raralabs/canal/core/message/content"
	"github.com/raralabs/canal/core/transforms/agg"
	"github.com/raralabs/canal/utils/cast"
	stream_math "github.com/raralabs/canal/utils/stream-math"
)

func NewCorrelation(alias, field1, field2 string, filter func(map[string]interface{}) bool) *AggTemplate {
	if alias == "" {
		alias = "correlation"
	}

	ag := NewAggTemplate(alias, field1, filter)

	ag.function = func() agg.IAggFunc {
		return newCorrelationFunc(ag, func() string {
			return field2
		})
	}

	return ag
}

type correlation struct {
	tmpl agg.IAggFuncTemplate

	cor    *stream_math.Correlation
	field2 func() string
}

func newCorrelationFunc(tmpl agg.IAggFuncTemplate, field2 func() string) *correlation {
	return &correlation{
		tmpl:   tmpl,
		cor:    stream_math.NewCorrelation(),
		field2: field2,
	}
}

func (c *correlation) Remove(prevContent content.IContent) {
	if prevContent != nil {
		vl1, ok1 := prevContent.Get(c.tmpl.Field())
		vl2, ok2 := prevContent.Get(c.field2())

		if ok1 && ok2 {
			x2, _ := cast.TryFloat(vl1.Val)
			y2, _ := cast.TryFloat(vl2.Val)

			c.cor.Remove(x2, y2)
		}
	}
}

func (c *correlation) Add(cntnt content.IContent) {
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

			c.cor.Add(x1, y1)
		}
	}
}

func (c *correlation) Result() content.MsgFieldValue {
	res, err := c.cor.Result()
	if err != nil {
		return content.NewFieldValue(nil, content.NONE)
	}
	return content.NewFieldValue(res, content.FLOAT)
}

func (c *correlation) Name() string {
	return c.tmpl.Name()
}

func (c *correlation) Reset() {
	c.cor.Reset()
}
