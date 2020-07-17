package templates

import (
	"github.com/raralabs/canal/core/transforms/agg"
	"github.com/raralabs/canal/ext/transforms/aggregates/functions"
)

func NewCovariance(alias, field1, field2 string, filter func(map[string]interface{}) bool) *AggTemplate {
	if alias == "" {
		alias = "Cov"
	}

	ag := NewAggTemplate(alias, field1, filter)

	ag.function = func() agg.IAggFunc {
		return functions.NewCovariance(ag, func() string {
			return field2
		})
	}

	return ag
}
