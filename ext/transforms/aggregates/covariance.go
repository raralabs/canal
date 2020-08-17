package aggregates

import (
	"github.com/raralabs/canal/core/message/content"
	"github.com/raralabs/canal/core/transforms/agg"
	"github.com/raralabs/canal/utils/cast"
	stream_math "github.com/raralabs/canal/utils/stream-math"
)

func NewCovariance(alias, field1, field2 string, filter func(map[string]interface{}) bool) *AggTemplate {
	if alias == "" {
		alias = "cov"
	}

	ag := NewAggTemplate(alias, field1, filter)

	ag.function = func() agg.IAggFunc {
		return newCovarianceFunc(ag, func() string {
			return field2
		})
	}

	return ag
}


type covariance struct {
	tmpl agg.IAggFuncTemplate

	cov    *stream_math.Covariance
	field2 func() string
}

func newCovarianceFunc(tmpl agg.IAggFuncTemplate, field2 func() string) *covariance {
	return &covariance{
		tmpl:   tmpl,
		cov:    stream_math.NewCovariance(),
		field2: field2,
	}
}

func (c *covariance) Remove(prevContent content.IContent) {
	if prevContent != nil {
		vl1, ok1 := prevContent.Get(c.tmpl.Field())
		vl2, ok2 := prevContent.Get(c.field2())

		if ok1 && ok2 {
			x2, _ := cast.TryFloat(vl1.Val)
			y2, _ := cast.TryFloat(vl2.Val)

			c.cov.Remove(x2, y2)
		}
	}
}

func (c *covariance) Add(cntnt content.IContent) {
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

			c.cov.Add(x1, y1)
		}
	}
}

func (c *covariance) Result() *content.MsgFieldValue {
	res, _ := c.cov.Result()
	return content.NewFieldValue(res, content.FLOAT)
}

func (c *covariance) Name() string {
	return c.tmpl.Name()
}

func (c *covariance) Reset() {
	c.cov.Reset()
}
