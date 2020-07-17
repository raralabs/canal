package templates

import (
	"github.com/raralabs/canal/core/transforms/agg"
	"github.com/raralabs/canal/ext/transforms/aggregates/functions"
)

func NewAvg(alias, field string, filter func(map[string]interface{}) bool) *AggTemplate {
	if alias == "" {
		alias = "Avg"
	}

	ag := NewAggTemplate(alias, field, filter)

	ag.function = func() agg.IAggFunc {
		return functions.NewAvg(ag)
	}

	return ag
}
