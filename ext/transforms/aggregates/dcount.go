package aggregates

import (
	"github.com/raralabs/canal/core/message"
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

func (c *dcount) Remove(prevContent *message.OrderedContent) {
	// Remove the previous fieldVal
	if prevContent != nil {
		if prevVal, ok := prevContent.Get(c.tmpl.Field()); ok {
			c.fqCnt.Remove(prevVal.Value())
		}
	}
}

func (c *dcount) Add(content *message.OrderedContent) {

	if c.tmpl.Filter(content.Values()) {

		val, ok := content.Get(c.tmpl.Field())
		if !ok {
			return
		}

		k := val.Value()
		c.fqCnt.Add(k)
	}
}

func (c *dcount) Result() *message.MsgFieldValue {
	dcnt, err := c.calculate(c.fqCnt.Values())
	if err != nil {
		return message.NewFieldValue(nil, message.NONE)
	}

	return message.NewFieldValue(dcnt, message.INT)
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
