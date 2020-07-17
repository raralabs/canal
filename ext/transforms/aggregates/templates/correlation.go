package templates

import (
	"github.com/raralabs/canal/core/transforms/agg"
	"github.com/raralabs/canal/ext/transforms/aggregates/functions"
)

func NewCorrelation(alias, field1, field2 string, filter func(map[string]interface{}) bool) *AggTemplate {
	if alias == "" {
		alias = "Correlation"
	}

	ag := NewAggTemplate(alias, field1, filter)

	ag.function = func() agg.IAggFunc {
		return functions.NewCorrelation(ag, func() string {
			return field2
		})
	}

	return ag
}
