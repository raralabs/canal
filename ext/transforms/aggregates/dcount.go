package aggregates

import (
	"github.com/raralabs/canal/core/message/content"
	"github.com/raralabs/canal/core/transforms/agg"
	stream_math "github.com/raralabs/canal/utils/stream-math"
)

func NewDCount(alias, field string, filter func(map[string]interface{}) bool) *AggTemplate {
	if alias == "" {
		alias = "dcount"
	}

	ag := NewAggTemplate(alias, field, filter)

	ag.function = func() agg.IAggFunc { return newDCountFunc(ag) }

	return ag
}


type dcount struct {
	tmpl  agg.IAggFuncTemplate
	fqCnt *stream_math.FreqCounter
}

func newDCountFunc(tmpl agg.IAggFuncTemplate) *dcount {
	return &dcount{
		tmpl:  tmpl,
		fqCnt: stream_math.NewFreqCounter(),
	}
}

func (c *dcount) Remove(prevContent content.IContent) {
	// Remove the previous fieldVal
	if prevContent != nil {
		if prevVal, ok := prevContent.Get(c.tmpl.Field()); ok {
			c.fqCnt.Remove(prevVal.Value())
		}
	}
}

func (c *dcount) Add(cntnt content.IContent) {

	if c.tmpl.Filter(cntnt.Values()) {

		val, ok := cntnt.Get(c.tmpl.Field())
		if !ok {
			return
		}

		k := val.Value()
		c.fqCnt.Add(k)
	}
}

func (c *dcount) Result() content.MsgFieldValue {
	dcnt, err := c.calculate(c.fqCnt.Values())
	if err != nil {
		return content.NewFieldValue(nil, content.NONE)
	}

	return content.NewFieldValue(dcnt, content.INT)
}

func (c *dcount) Name() string {
	return c.tmpl.Name()
}

func (c *dcount) Reset() {
	c.fqCnt.Reset()
}

func (c *dcount) calculate(m map[interface{}]uint64) (uint64, error) {
	dcnt := uint64(0)

	for _, v := range m {
		if v > 0 {
			dcnt++
		}
	}

	return dcnt, nil
}
