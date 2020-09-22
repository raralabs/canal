package regparser

import (
	"regexp"
)

//Gets the compiled regex rule and message as parameters and returns the extracted
//fields or params from message specified by regex along with their corresponding
//values in a map as map[first_name:abc age:10] for regex of type
//(?P<first_name>\w+).*?am\s+(?P<age>\d+)

func ExtractParams(regEx *regexp.Regexp, msg string) (paramsMap map[string]string) {

	matchedSubString := regEx.FindStringSubmatch(msg)
	paramsMap = make(map[string]string)

	for idx, name := range regEx.SubexpNames() {
		if idx > 0 && idx <= len(matchedSubString) {
			paramsMap[name] = matchedSubString[idx]
		}
	}
	return paramsMap
}
