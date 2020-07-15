package functions

import (
	"github.com/raralabs/canal/core/message"
	"github.com/raralabs/canal/core/transforms/agg"
	stream_math "github.com/raralabs/canal/utils/stream-math"
)

type Mode struct {
	tmpl  agg.IAggFuncTemplate
	fqCnt *stream_math.FreqCounter
	valType message.FieldValueType
	first   bool
}

func NewMode(tmpl agg.IAggFuncTemplate) *Mode {
	return &Mode{
		tmpl:  tmpl,
		valType: message.NONE,
		first:   true,
		fqCnt: stream_math.NewFreqCounter(),
	}
}

func (c *Mode) Add(content, prevContent *message.OrderedContent) {
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

		if c.first {
			c.first = false
			c.valType = val.ValueType()
		}

		k := val.Value()
		c.fqCnt.Add(k)
	}
}

func (c *Mode) Result() *message.MsgFieldValue {
	mode, err := c.calculate(c.fqCnt.Values())
	if err != nil {
		return message.NewFieldValue(nil, message.NONE)
	}

	return message.NewFieldValue(mode, c.valType)
}

func (c *Mode) Name() string {
	return c.tmpl.Name()
}

func (c *Mode) Reset() {
	c.fqCnt.Reset()
}

func (c *Mode) calculate(m map[interface{}]uint64) (interface{}, error) {

	var mode interface{}
	mx := uint64(0)
	for k, v := range m {
		if v > 0 {
			if v > mx {
				mx = v
				mode = k
			}
		}
	}

	return mode, nil
}
