package joinUsingHshMap

import (
	"fmt"
	"github.com/raralabs/canal/core/message"
	"github.com/raralabs/canal/core/message/content"
	"github.com/raralabs/canal/utils/regparser"
	"regexp"
	"strings"
)

type innerJoin struct{
	joinType   		JoinType
	hashTable  		*HashTable
	streamContainer []message.Msg
	mergedContent 	[]content.IContent
	JoinStrategy	JoinStrategy
}


func NewInnerJoin(strategy JoinStrategy)*innerJoin{
	switch strategy {
	case HASH://join using hash table
		return &innerJoin{hashTable: NewHashMap(),
			mergedContent: []content.IContent{},JoinStrategy:HASH}
	case TABLE://join using temporal table
		return &innerJoin{hashTable: NewHashMap(),
			mergedContent: []content.IContent{},JoinStrategy: TABLE}
	default:
		return &innerJoin{hashTable: NewHashMap(),
			mergedContent: []content.IContent{},JoinStrategy: HASH}
	}
}

func(in *innerJoin)Type()JoinType{
	return in.joinType
}

func(in *innerJoin)Join(inStream1,inStream2 content.IContent,selectFields []string)content.IContent{
	dataTypeTracker1 := inStream1.Types()
	dataTypeTracker2 := inStream2.Types()
	messageContent1 := inStream1.Values()
	messageContent2 := inStream2.Values()
	newMsgContent := content.New()
	if selectFields[0]!="*" {
		for _, key := range selectFields {
			newMsgField1, ok1 := messageContent1[key]
			if ok1 {
				newMsgContent.Add(key, content.NewFieldValue(newMsgField1, dataTypeTracker1[key]))
			}
			newMsgField2, ok2 := messageContent2[key]
			if ok2 {
				newMsgContent.Add(key, content.NewFieldValue(newMsgField2, dataTypeTracker2[key]))
			}
			if !(ok1 || ok2){
				pErr:= fmt.Sprintf("failed to get specified field,neither streams contains field: %s",key)
				panic(pErr)
			}
		}
	}else{
		for key,value := range messageContent1{
			newMsgContent.Add(key, content.NewFieldValue(value, dataTypeTracker1[key]))
		}
		for key,value := range messageContent2{
			newMsgContent.Add(key, content.NewFieldValue(value, dataTypeTracker2[key]))
		}
	}

	//in.mergedContent = append(in.mergedContent,merged)
	return newMsgContent
}

func(in *innerJoin)ProcessStreamFirst(msg content.IContent,fieldsFromStream1 []string){
	if in.JoinStrategy == HASH{
		var joinFieldsVal []interface{}
		for _,field := range fieldsFromStream1{
			joinFieldsVal= append(joinFieldsVal,msg.Values()[strings.TrimSpace(field)])
		}
		key := concatKeys(joinFieldsVal)
		in.hashTable.Set(msg,key)
	}
}
func(in *innerJoin)ProcessStreamSec(msg content.IContent,fieldsFromStream2 []string)(interface{},bool){
	if in.JoinStrategy == HASH{
		var joinFieldsVal []interface{}

		for _,field := range fieldsFromStream2{
			joinFieldsVal= append(joinFieldsVal,msg.Values()[strings.TrimSpace(field)])
		}

		key := concatKeys(joinFieldsVal)
		result,ok:= in.hashTable.Get(key)
		return result,ok
	}
	return nil,false
}
//not used in the current version of the code
func(in *innerJoin) Condition(query string)([]string,[]string){
	var cleanedFields1 []string //to hold join keys for the first stream
	var cleanedFields2 []string // to hold the join keys for the second stream
	regEx,_ := regexp.Compile(`ON\s+?(?P<seg1>[^=]+)\s?\=\s+?(?P<seg2>[^\s\s\s+]+)`)
	params := regparser.ExtractParams(regEx,query)

	rawFields1 := strings.Split(params["seg1"], ",")
	rawFields2 := strings.Split(params["seg2"], ",")

	for _,field := range rawFields1{
		reqField := strings.Split(field,".")[1]
		cleanedFields1 = append(cleanedFields1,strings.TrimSpace(reqField))
	}
	for _,field := range rawFields2{
		reqField := strings.Split(field,".")[1]
		cleanedFields2 = append(cleanedFields2,reqField)
	}
	return cleanedFields1,cleanedFields2
}





