package templates

import (
	"fmt"
	"log"

	"github.com/raralabs/canal/core/transforms/agg"
	"github.com/raralabs/canal/ext/transforms/aggregates/functions"
)

func NewQuantile(alias, field string, q float64, filter func(map[string]interface{}) bool) *AggTemplate {

	if q < 0 || q > 1 {
		log.Panic("Quantile range must be (0,1)")
	}

	if alias == "" {
		alias = fmt.Sprintf("%.2fth Quantile", q*100)
	}

	ag := NewAggTemplate(alias, field, filter)

	ag.function = func() agg.IAggFunc {
		return functions.NewQuantile(ag, func() float64 {
			return q
		})
	}

	return ag
}
