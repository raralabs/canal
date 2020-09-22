package joinUsingHshMap

import (
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
			mergedContent: []content.IContent{},JoinStrategy: WINDOW}
	}
}

func(in *innerJoin)Type()JoinType{
	return in.joinType
}

func(in *innerJoin)Join(inStream1,inStream2 content.IContent)content.IContent{
	merged := mergeContent(inStream1,inStream2)
	in.mergedContent = append(in.mergedContent,merged)
	return merged
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




//func Start(){
//	//dummy query example
//	query := "SELECT * FROM Stream1 INNERJOIN Stream2 ON Stream1.age,Stream1.first_name,Stream1. last_name = Stream2.age,Stream2.full_name"
//	//creates a new inner join
//	newJoin := NewInnerJoin(HASH)
//
//	//prepares query by extractiong the join keys and condition
//	fieldsFromStream1,fieldsFromStream2 := newJoin.Condition(query)
//
//	//channels for message stream 1 and 2 to mimick the messages from path1 and path2
//	messageStream1 := make(chan content.IContent)
//	messageStream2 := make(chan content.IContent)
//
//	go createMsgForPath1(100,messageStream1)
//	go createMsgForPath2(100,messageStream2)
//
//	//get the messages from stream1 and hold them in hash map
//	go func() {
//		for msg := range messageStream1{
//			newJoin.ProcessStreamFirst(msg,fieldsFromStream1)
//		}
//	}()
//	//get the message from stream2 and check for the match in the hashmap
//	count:=0
//	for msg := range messageStream2{
//		result,ok:=newJoin.ProcessStreamSec(msg,fieldsFromStream2)
//		if ok{
//			newJoin.Join(result.(content.IContent),msg)
//		}else{
//			count++
//			//fmt.Println(count,"not matched")
//		}
//
//	}
//	fmt.Println(newJoin.mergedContent,len(newJoin.mergedContent),"joined")
//}

