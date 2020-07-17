package functions

import (
	"github.com/raralabs/canal/core/message"
	"github.com/raralabs/canal/core/transforms/agg"
	"github.com/raralabs/canal/utils/cast"
	sort_algo "github.com/raralabs/canal/utils/sort-algo"
	stream_math "github.com/raralabs/canal/utils/stream-math"
	"math"
)

type Quantile struct {
	tmpl  agg.IAggFuncTemplate
	qth   func() float64
	fqCnt *stream_math.FreqCounter

	orderedVals *sort_algo.Insertion
}

func NewQuantile(tmpl agg.IAggFuncTemplate, qth func() float64) *Quantile {

	return &Quantile{
		tmpl:  tmpl,
		qth:   qth,
		fqCnt: stream_math.NewFreqCounter(),
		orderedVals: sort_algo.NewInsertion(func(old, new interface{}) bool {
			o, _ := cast.TryFloat(old)
			n, _ := cast.TryFloat(new)

			return o > n
		}),
	}
}

func (q *Quantile) Remove(prevContent *message.OrderedContent) {
	// Remove the previous fieldVal
	if prevContent != nil {
		if prevVal, ok := prevContent.Get(q.tmpl.Field()); ok {
			switch prevVal.ValueType() {
			case message.INT, message.FLOAT:
				val, _ := cast.TryFloat(prevVal.Value())
				if v := q.fqCnt.Remove(val); v != nil {
					q.orderedVals.Remove(v)
				}
			}
		}
	}
}

func (q *Quantile) Add(content, prevContent *message.OrderedContent) {

	if q.tmpl.Filter(content.Values()) {

		val, ok := content.Get(q.tmpl.Field())
		if !ok {
			return
		}

		switch val.ValueType() {
		case message.INT, message.FLOAT:
			vl, _ := cast.TryFloat(val.Value())
			if v := q.fqCnt.Add(vl); v != nil {
				q.orderedVals.Add(v)
			}
		}
	}
}

func (q *Quantile) Result() *message.MsgFieldValue {
	res := q.quantile()
	return message.NewFieldValue(res, message.FLOAT)
}

func (q *Quantile) quantile() float64 {
	qth := q.qth()
	n := q.fqCnt.TotalFreq()

	i := qth * float64(n+1)
	j := math.Floor(i)

	values := q.fqCnt.Values()
	var jth, j1th interface{}

	cf := uint64(0)
	for e := q.orderedVals.First(); e != nil; e = e.Next() {
		entry := e.Value
		cf += values[entry]

		if (j+1) <= float64(cf) {
			j1th = entry
			break
		}

		if j <= float64(cf) {
			jth = entry
			if i == j {
				jthf, _ := cast.TryFloat(jth)
				return jthf
			}
		}
	}

	if jth == nil {
		jth = j1th
	}

	jthf, _ := cast.TryFloat(jth)
	j1thf, _ := cast.TryFloat(j1th)

	return jthf + (j1thf - jthf)*(i - j)
}

func (q *Quantile) Name() string {
	return q.tmpl.Name()
}

func (q *Quantile) Reset() {
}
