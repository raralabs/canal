package aggregates

import (
	"github.com/raralabs/canal/core/message/content"
	"github.com/raralabs/canal/core/transforms/agg"
	"github.com/raralabs/canal/utils/cast"
	stream_math "github.com/raralabs/canal/utils/stream-math"
)

func NewVariance(alias, field string, filter func(map[string]interface{}) bool) *AggTemplate {
	if alias == "" {
		alias = "var"
	}

	ag := NewAggTemplate(alias, field, filter)

	ag.function = func() agg.IAggFunc { return newVarianceFunc(ag) }

	return ag
}

type variance struct {
	tmpl agg.IAggFuncTemplate

	variance *stream_math.Variance
}

func newVarianceFunc(tmpl agg.IAggFuncTemplate) *variance {
	return &variance{
		tmpl:     tmpl,
		variance: stream_math.NewVariance(),
	}
}

func (c *variance) Remove(prevContent content.IContent) {
	if prevContent != nil {
		if old, ok := prevContent.Get(c.tmpl.Field()); ok {
			v1, _ := cast.TryFloat(old.Value())
			c.variance.Remove(v1)
			return
		}
	}
}

func (c *variance) Add(cntnt content.IContent) {
	if c.tmpl.Filter(cntnt.Values()) {
		val, ok := cntnt.Get(c.tmpl.Field())
		if !ok {
			return
		}

		switch val.ValueType() {
		case content.INT, content.FLOAT:
			v, _ := cast.TryFloat(val.Value())
			c.variance.Add(v)
		}
	}
}

func (c *variance) Result() *content.MsgFieldValue {
	res, err := c.variance.Result()
	if err != nil {
		return content.NewFieldValue(nil, content.NONE)
	}
	return content.NewFieldValue(res, content.FLOAT)
}

func (c *variance) Name() string {
	return c.tmpl.Name()
}

func (c *variance) Reset() {
	c.variance.Reset()
}
