package aggregates

import (
	"github.com/raralabs/canal/core/message/content"
	"github.com/raralabs/canal/core/transforms/agg"
	"github.com/raralabs/canal/utils/cast"
	stream_math "github.com/raralabs/canal/utils/stream-math"
)


func NewAvg(alias, field string, filter func(map[string]interface{}) bool) *AggTemplate {
	if alias == "" {
		alias = "avg"
	}

	ag := NewAggTemplate(alias, field, filter)

	ag.function = func() agg.IAggFunc {
		return newAvgFunc(ag)
	}

	return ag
}


type avg struct {
	tmpl agg.IAggFuncTemplate
	avg *stream_math.Mean
}

func newAvgFunc(tmpl agg.IAggFuncTemplate) *avg {
	return &avg{
		tmpl: tmpl,
		avg:  stream_math.NewMean(),
	}
}

func (c *avg) Remove(prevContent content.IContent) {
	if prevContent != nil {
		if old, ok := prevContent.Get(c.tmpl.Field()); ok {
			v1, _ := cast.TryFloat(old.Value())
			c.avg.Remove(v1)
			return
		}
	}
}

func (c *avg) Add(cntnt content.IContent) {
	if c.tmpl.Filter(cntnt.Values()) {
		val, ok := cntnt.Get(c.tmpl.Field())
		if !ok {
			return
		}

		switch val.ValueType() {
		case content.INT, content.FLOAT:
			v, _ := cast.TryFloat(val.Value())
			c.avg.Add(v)
		}
	}
}

func (c *avg) Result() *content.MsgFieldValue {
	res, _ := c.avg.Result()
	return content.NewFieldValue(res, content.FLOAT)
}

func (c *avg) Name() string {
	return c.tmpl.Name()
}

func (c *avg) Reset() {
	c.avg.Reset()
}
