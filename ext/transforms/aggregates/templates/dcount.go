package templates

import (
	"github.com/raralabs/canal/core/transforms/agg"
	"github.com/raralabs/canal/ext/transforms/aggregates/functions"
)

type DCount struct {
	name   string
	filter func(map[string]interface{}) bool
	field  string
}

func NewDCount(alias, field string, filter func(map[string]interface{}) bool) *DCount {
	if alias == "" {
		alias = "Distinct_Count"
	}
	return &DCount{
		name:   alias,
		filter: filter,
		field:  field,
	}
}

func (H *DCount) Name() string {
	return H.name
}

func (H *DCount) Field() string {
	return H.field
}

func (H *DCount) Filter(m map[string]interface{}) bool {
	return H.filter(m)
}

func (H *DCount) Function() agg.IAggFunc {
	return functions.NewDCount(H)
}
