package aggregates

import (
	"github.com/raralabs/canal/core/message/content"
	"github.com/raralabs/canal/core/transforms/agg"
	"github.com/raralabs/canal/utils/cast"
	sort_algo "github.com/raralabs/canal/utils/sort-algo"
	stream_math "github.com/raralabs/canal/utils/stream-math"
)


func NewMode(alias, field string, filter func(map[string]interface{}) bool) *AggTemplate {
	if alias == "" {
		alias = "mode"
	}

	ag := NewAggTemplate(alias, field, filter)

	ag.function = func() agg.IAggFunc { return newModeFunc(ag) }

	return ag
}

type mode struct {
	tmpl        agg.IAggFuncTemplate
	fqCnt       *stream_math.FreqCounter
	valType     content.FieldValueType
	first       bool
	orderedVals *sort_algo.Insertion
}

func newModeFunc(tmpl agg.IAggFuncTemplate) *mode {
	return &mode{
		tmpl:    tmpl,
		valType: content.NONE,
		first:   true,
		fqCnt:   stream_math.NewFreqCounter(),
		orderedVals: sort_algo.NewInsertion(func(old, new interface{}) bool {
			o, _ := cast.TryFloat(old)
			n, _ := cast.TryFloat(new)

			return o > n
		}),
	}
}

func (md *mode) Remove(prevContent content.IContent) {
	// Remove the previous fieldVal
	if prevContent != nil {
		if prevVal, ok := prevContent.Get(md.tmpl.Field()); ok {
			switch prevVal.ValueType() {
			case content.INT, content.FLOAT:
				val, _ := cast.TryFloat(prevVal.Value())
				if v := md.fqCnt.Remove(val); v != nil {
					md.orderedVals.Remove(v)
				}
			}
		}
	}
}

func (md *mode) Add(cntnt content.IContent) {

	if md.tmpl.Filter(cntnt.Values()) {

		val, ok := cntnt.Get(md.tmpl.Field())
		if !ok {
			return
		}

		switch val.ValueType() {
		case content.INT, content.FLOAT:
			vl, _ := cast.TryFloat(val.Value())
			if v := md.fqCnt.Add(vl); v != nil {
				md.orderedVals.Add(v)
			}
		}
	}
}

func (md *mode) Result() *content.MsgFieldValue {
	mode, err := md.calculate(md.fqCnt.Values())
	if err != nil {
		return content.NewFieldValue(nil, content.NONE)
	}

	return content.NewFieldValue(mode, md.valType)
}

func (md *mode) Name() string {
	return md.tmpl.Name()
}

func (md *mode) Reset() {
	md.fqCnt.Reset()
}

func (md *mode) calculate(m map[interface{}]uint64) (interface{}, error) {

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
