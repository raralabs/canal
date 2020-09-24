package joinqryparser

import (
	"regexp"
	"strings"
)

//tokenizes the sentence based on the tkDecider
func tokenize(sentence string,tkDecider string)[]string{
	var tokens []string
	for _,token := range strings.Split(sentence,tkDecider){
		tokens = append(tokens,token)
	}
	return tokens
}

//extracts the join parameters and condition
func getJoinCondition(querySeg string)(string,string,string) {
	space := regexp.MustCompile(`\s`)
	querySeg=space.ReplaceAllString(querySeg, "")
	switchFlag := false
	firstEnd := 0
	secondStart := 0
	for idx, character := range querySeg {
		if character == '='||character=='<'||character=='>' {
			switchFlag = true
			continue
		} else {
			if switchFlag == false {
				firstEnd++
			} else if switchFlag == true {
				secondStart = idx
				break
			}
		}
	}
	fields1 := querySeg[:firstEnd]
	fields2 := querySeg[secondStart:len(querySeg)]
	operator := querySeg[firstEnd:secondStart]
	return fields1,fields2,operator
}

