package templates

import (
	"github.com/raralabs/canal/core/transforms/agg"
	"github.com/raralabs/canal/ext/transforms/aggregates/functions"
)

type Covariance struct {
	name   string
	filter func(map[string]interface{}) bool
	field1  string
	field2  string
}

func NewCovariance(alias, field1, field2 string, filter func(map[string]interface{}) bool) *Covariance {
	if alias == "" {
		alias = "Cov"
	}
	return &Covariance{
		name:   alias,
		filter: filter,
		field1:  field1,
		field2: field2,
	}
}

func (cv *Covariance) Filter(m map[string]interface{}) bool {
	return cv.filter(m)
}

func (cv *Covariance) Function() agg.IAggFunc {
	return functions.NewCovariance(cv, cv.Field2)
}

func (cv *Covariance) Name() string {
	return cv.name
}

func (cv *Covariance) Field() string {
	return cv.field1
}

func (cv *Covariance) Field2() string {
	return cv.field2
}
