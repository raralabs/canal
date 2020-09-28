package joinqryparser

import (
	"github.com/raralabs/canal/ext/transforms/joinUsingHshMap"
	"github.com/raralabs/canal/utils/regparser"
	"regexp"
	"strings"
)

//data structure to hold the name of stream and their alias if provided
type tableValues struct{
	alias 		string
	Name        string
}

//data structure to hold the condition for the join, left equation,operator and right equation
//of the query
type condition struct{
	Fields1    []string
	Fields2	   []string
	Operator  string
}

//to hold the intermediate parsed values
type queryParser struct{
	query		string
	SubType		joinUsingHshMap.JoinSubType
	Select 		[]string
	FirstTable	tableValues
	JoinType    joinUsingHshMap.JoinType
	SecondTable tableValues
	Condition	condition

}
//create a new query parser with the parser initialize with the query parser
func NewQueryParser(query string)*queryParser{
	space := regexp.MustCompile(`\s+`)
	cleanedQuery := space.ReplaceAllString(query, " ")
	return &queryParser{query:cleanedQuery}
}


//function tokenizes query into 3 parts select fields,from tables and conditions"
func(qp *queryParser) PrepareQuery()*queryParser{
	exp := `(SELECT|select)\s+(?P<select>.+)\s+(FROM|from)\s+(?P<table1>\w+\s?\w+?)\s(?P<joinType>[A-Za-z]+(join|JOIN))\s(?P<table2>\w+\s?\w+?)\s(on|ON)\s(?P<condition>.+)`
	reg,err := regexp.Compile(exp)
	if err!=nil{
		panic("cannot compile regex in joinqryparser")
	}
	fields :=  regparser.ExtractParams(reg,qp.query)

	for key,field := range fields{
		switch key{
		case "select":
			qp.Select=tokenize(field,",")
		case "table1":
			exp := `(?P<name>\w+)(\s(as|AS))?(\s(?P<alias>\w+))?`
			reg,err :=regexp.Compile(exp)
			if err!= nil{
				panic("some thing went wrong while parsing query might be syntax error ")
			}
			params := regparser.ExtractParams(reg,field)
			for key,value := range params{
				if key ==  "name"{
					qp.FirstTable.Name = value
				}else if key== "alias"{
					qp.FirstTable.alias = value
				}
			}

		case "table2":

			exp := `(?P<name>\w+)(\s(as|AS))?(\s(?P<alias>\w+))?`
			reg,err :=regexp.Compile(exp)
			if err!= nil{
				panic("some thing went wrong while parsing query might be syntax error ")
			}
			params := regparser.ExtractParams(reg,field)
			for key,value := range params{

				if key ==  "name"{
					qp.SecondTable.Name = value
				}else if key== "alias"{
					qp.SecondTable.alias = value
				}
			}


		case "joinType":
			joinType := strings.ToUpper(field)

			switch joinType{
			case "INNERJOIN":
				qp.JoinType=joinUsingHshMap.INNER
			case "LEFTOUTERJOIN":
				qp.JoinType = joinUsingHshMap.OUTER
				qp.SubType = joinUsingHshMap.LEFTOUTER
			case "RIGHTOUTERJOIN":
				qp.JoinType = joinUsingHshMap.OUTER
				qp.SubType = joinUsingHshMap.RIGHTOUTER
			default:
				panic("join type not recognized, currenlty support for INNERJOIN and LEFTOUTERJOIN")
			}

		case "condition":
			fields1,fields2,operator := getJoinCondition(field)
			qp.Condition.Operator = operator
			tokenizedFields1 := tokenize(fields1,",")
			tokenizedFields2 := tokenize(fields2,",")
			for _,token:= range(tokenizedFields1){
				AliasedField := tokenize(token,".")
				if len(AliasedField)>1{
					qp.Condition.Fields1 =append(qp.Condition.Fields1,strings.TrimSpace(AliasedField[1]))
				}else{
					qp.Condition.Fields1 = append(qp.Condition.Fields1,strings.TrimSpace(AliasedField[0]))
				}
			}
			for _,token:= range(tokenizedFields2){
				AliasedField := tokenize(token,".")
				if len(AliasedField)>1{
					qp.Condition.Fields2 =append(qp.Condition.Fields2,strings.TrimSpace(AliasedField[1]))
				}else{
					qp.Condition.Fields2 = append(qp.Condition.Fields2,strings.TrimSpace(AliasedField[0]))
				}
			}
			default:
		}
	}
	return qp
}


