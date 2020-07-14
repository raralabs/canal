package functions

import (
	"github.com/raralabs/canal/core/message"
	"github.com/raralabs/canal/core/transforms/agg"
	"github.com/raralabs/canal/utils/cast"
	"github.com/spenczar/tdigest"
)

type Quantile struct {
	tmpl agg.IAggFuncTemplate
	tdg  *tdigest.TDigest
	wtFn func() string
	qth  func() float64
}

func NewQuantile(tmpl agg.IAggFuncTemplate, wtFn func() string, qth func() float64) *Quantile {

	return &Quantile{
		tmpl: tmpl,
		wtFn: wtFn,
		qth:  qth,
		tdg:  tdigest.New(),
	}
}

func (q *Quantile) Add(content, prevContent *message.OrderedContent) {
	if q.tmpl.Filter(content.Values()) {
		val, ok := content.Get(q.tmpl.Field())
		if !ok {
			return
		}

		vf, ok := cast.TryFloat(val.Val)
		if !ok {
			return
		}

		wt := q.wtFn()
		if wt != "" {
			if weight, ok := content.Get(wt); ok {
				wf, ok := cast.TryInt(weight.Val)

				if !ok {
					return
				}

				q.tdg.Add(vf, int(wf))
			}
		} else {
			q.tdg.Add(vf, 1)
		}
	}
}

func (q *Quantile) Result() *message.MsgFieldValue {
	return message.NewFieldValue(q.tdg.Quantile(q.qth()), message.FLOAT)
}

func (q *Quantile) Name() string {
	return q.tmpl.Name()
}

func (q *Quantile) Reset() {
	q.tdg = tdigest.New()
}
