package regparser

import (
	"regexp"
)

func ValidateData(regEx *regexp.Regexp, field string)bool{
	matched := regEx.MatchString(field)
	return matched
}
