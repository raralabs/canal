package functions

import (
	"github.com/raralabs/canal/core/message"
	"github.com/raralabs/canal/core/transforms/agg"
	"github.com/raralabs/canal/utils/cast"
	sort_algo "github.com/raralabs/canal/utils/sort-algo"
	stream_math "github.com/raralabs/canal/utils/stream-math"
)

type Mode struct {
	tmpl    agg.IAggFuncTemplate
	fqCnt   *stream_math.FreqCounter
	valType message.FieldValueType
	first   bool
	orderedVals *sort_algo.Insertion
}

func NewMode(tmpl agg.IAggFuncTemplate) *Mode {
	return &Mode{
		tmpl:    tmpl,
		valType: message.NONE,
		first:   true,
		fqCnt:   stream_math.NewFreqCounter(),
		orderedVals: sort_algo.NewInsertion(func(old, new interface{}) bool {
			o, _ := cast.TryFloat(old)
			n, _ := cast.TryFloat(new)

			return o > n
		}),
	}
}

func (md *Mode) Add(content, prevContent *message.OrderedContent) {
	// Remove the previous fieldVal
	if prevContent != nil {
		if prevVal, ok := prevContent.Get(md.tmpl.Field()); ok {
			switch prevVal.ValueType() {
			case message.INT, message.FLOAT:
				val, _ := cast.TryFloat(prevVal.Value())
				if v := md.fqCnt.Remove(val); v != nil {
					md.orderedVals.Remove(v)
				}
			}
		}
	}

	if md.tmpl.Filter(content.Values()) {

		val, ok := content.Get(md.tmpl.Field())
		if !ok {
			return
		}

		switch val.ValueType() {
		case message.INT, message.FLOAT:
			vl, _ := cast.TryFloat(val.Value())
			if v := md.fqCnt.Add(vl); v != nil {
				md.orderedVals.Add(v)
			}
		}
	}
}

func (md *Mode) Result() *message.MsgFieldValue {
	mode, err := md.calculate(md.fqCnt.Values())
	if err != nil {
		return message.NewFieldValue(nil, message.NONE)
	}

	return message.NewFieldValue(mode, md.valType)
}

func (md *Mode) Name() string {
	return md.tmpl.Name()
}

func (md *Mode) Reset() {
	md.fqCnt.Reset()
}

func (md *Mode) calculate(m map[interface{}]uint64) (interface{}, error) {

	var mode interface{}
	mx := uint64(0)
	for e := md.orderedVals.First(); e != nil; e = e.Next() {
		k := e.Value
		v := m[k]
		if v > 0 {
			if v > mx {
				mx = v
				mode = k
			}
		}
	}

	return mode, nil
}
