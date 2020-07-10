package templates

import (
	"github.com/raralabs/canal/core/transforms/agg"
	"github.com/raralabs/canal/ext/transforms/aggregates/functions"
)

// HLLpp uses HyperLogLog++ to count distinct number
// of elements
type HLLpp struct {
	name   string
	filter func(map[string]interface{}) bool
	field  string
}

func NewHLLpp(alias, field string, filter func(map[string]interface{}) bool) *HLLpp {
	if alias == "" {
		alias = "Distinct_Count"
	}
	return &HLLpp{
		name:   alias,
		filter: filter,
		field:  field,
	}
}

func (H *HLLpp) Name() string {
	return H.name
}

func (H *HLLpp) Field() string {
	return H.field
}

func (H *HLLpp) Filter(m map[string]interface{}) bool {
	return H.filter(m)
}

func (H *HLLpp) Function() agg.IAggFunc {
	return functions.NewHLLpp(H)
}
