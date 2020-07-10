package templates

import (
	"fmt"
	"log"

	"github.com/raralabs/canal/core/transforms/agg"
	"github.com/raralabs/canal/ext/transforms/aggregates/functions"
)

type Quantile struct {
	name   string
	filter func(map[string]interface{}) bool
	field  string
	weight string
	q      float64
}

func NewQuantile(alias, field, weight string, q float64, filter func(map[string]interface{}) bool) *Quantile {

	if q < 0 || q > 1 {
		log.Panic("Quantile range must be (0,1)")
	}

	if alias == "" {
		alias = fmt.Sprintf("%fth Quantile", q*100)
	}
	return &Quantile{
		name:   alias,
		filter: filter,
		field:  field,
		weight: weight,
		q:      q,
	}
}

func (q *Quantile) Name() string {
	return q.name
}

func (q *Quantile) Field() string {
	return q.field
}

func (q *Quantile) Filter(m map[string]interface{}) bool {
	return q.filter(m)
}

func (q *Quantile) Function() agg.IAggFunc {
	return functions.NewQuantile(q, q.Weight, q.Qth)
}

func (q *Quantile) Weight() string {
	return q.weight
}

func (q *Quantile) Qth() float64 {
	return q.q
}
