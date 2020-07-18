package aggregates

import (
	"github.com/raralabs/canal/core/message"
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

func (c *variance) Remove(prevContent *message.OrderedContent) {
	if prevContent != nil {
		if old, ok := prevContent.Get(c.tmpl.Field()); ok {
			v1, _ := cast.TryFloat(old.Value())
			c.variance.Remove(v1)
			return
		}
	}
}

func (c *variance) Add(content *message.OrderedContent) {
	if c.tmpl.Filter(content.Values()) {
		val, ok := content.Get(c.tmpl.Field())
		if !ok {
			return
		}

		switch val.ValueType() {
		case message.INT, message.FLOAT:
			v, _ := cast.TryFloat(val.Value())
			c.variance.Add(v)
		}
	}
}

func (c *variance) Result() *message.MsgFieldValue {
	res, err := c.variance.Result()
	if err != nil {
		return message.NewFieldValue(nil, message.NONE)
	}
	return message.NewFieldValue(res, message.FLOAT)
}

func (c *variance) Name() string {
	return c.tmpl.Name()
}

func (c *variance) Reset() {
	c.variance.Reset()
}
