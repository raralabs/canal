package functions

import (
	"github.com/raralabs/canal/core/message"
	"github.com/raralabs/canal/core/transforms/agg"
	stream_math "github.com/raralabs/canal/utils/stream-math"
)

type DCount struct {
	tmpl  agg.IAggFuncTemplate
	fqCnt *stream_math.FreqCounter
}

func NewDCount(tmpl agg.IAggFuncTemplate) *DCount {
	return &DCount{
		tmpl:  tmpl,
		fqCnt: stream_math.NewFreqCounter(),
	}
}

func (c *DCount) Remove(prevContent *message.OrderedContent) {}

func (c *DCount) Add(content, prevContent *message.OrderedContent) {
	// Remove the previous fieldVal
	if prevContent != nil {
		if prevVal, ok := prevContent.Get(c.tmpl.Field()); ok {
			c.fqCnt.Remove(prevVal.Value())
		}
	}

	if c.tmpl.Filter(content.Values()) {

		val, ok := content.Get(c.tmpl.Field())
		if !ok {
			return
		}

		k := val.Value()
		c.fqCnt.Add(k)
	}
}

func (c *DCount) Result() *message.MsgFieldValue {
	dcnt, err := c.calculate(c.fqCnt.Values())
	if err != nil {
		return message.NewFieldValue(nil, message.NONE)
	}

	return message.NewFieldValue(dcnt, message.INT)
}

func (c *DCount) Name() string {
	return c.tmpl.Name()
}

func (c *DCount) Reset() {
	c.fqCnt.Reset()
}

func (c *DCount) calculate(m map[interface{}]uint64) (uint64, error) {
	dcnt := uint64(0)

	for _, v := range m {
		if v > 0 {
			dcnt++
		}
	}

	return dcnt, nil
}
