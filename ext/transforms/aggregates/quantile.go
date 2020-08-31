package aggregates

import (
	"fmt"
	"github.com/raralabs/canal/core/message/content"
	"log"
	"math"
	"strings"

	"github.com/raralabs/canal/core/transforms/agg"
	"github.com/raralabs/canal/utils/cast"
	sort_algo "github.com/raralabs/canal/utils/sort-algo"
	stream_math "github.com/raralabs/canal/utils/stream-math"
)

func NewQuantile(alias, field string, q float64, filter func(map[string]interface{}) bool) *AggTemplate {

	if q < 0 || q > 1 {
		log.Panic("quantile range must be (0,1)")
	}

	if alias == "" {
		qth := strings.TrimRight(strings.TrimRight(fmt.Sprintf("%.2f", q*100), "0"), ".")
		alias = fmt.Sprintf("%sth quantile", qth)
	}

	ag := NewAggTemplate(alias, field, filter)

	ag.function = func() agg.IAggFunc {
		return newQuantileFunc(ag, func() float64 {
			return q
		})
	}

	return ag
}

type quantile struct {
	tmpl  agg.IAggFuncTemplate
	qth   func() float64
	fqCnt *stream_math.FreqCounter

	orderedVals *sort_algo.Insertion
}

func newQuantileFunc(tmpl agg.IAggFuncTemplate, qth func() float64) *quantile {

	return &quantile{
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

func (q *quantile) Remove(prevContent content.IContent) {
	// Remove the previous fieldVal
	if prevContent != nil {
		if prevVal, ok := prevContent.Get(q.tmpl.Field()); ok {
			switch prevVal.ValueType() {
			case content.INT, content.FLOAT:
				val, _ := cast.TryFloat(prevVal.Value())
				if v := q.fqCnt.Remove(val); v != nil {
					q.orderedVals.Remove(v)
				}
			}
		}
	}
}

func (q *quantile) Add(cntnt content.IContent) {

	if q.tmpl.Filter(cntnt.Values()) {

		val, ok := cntnt.Get(q.tmpl.Field())
		if !ok {
			return
		}

		switch val.ValueType() {
		case content.INT, content.FLOAT:
			vl, _ := cast.TryFloat(val.Value())
			if v := q.fqCnt.Add(vl); v != nil {
				q.orderedVals.Add(v)
			}
		}
	}
}

func (q *quantile) Result() content.MsgFieldValue {
	res := q.quantile()
	return content.NewFieldValue(res, content.FLOAT)
}

func (q *quantile) quantile() float64 {
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

		if (j + 1) <= float64(cf) {
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

	return jthf + (j1thf-jthf)*(i-j)
}

func (q *quantile) Name() string {
	return q.tmpl.Name()
}

func (q *quantile) Reset() {
}
