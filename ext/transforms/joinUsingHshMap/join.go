package joinUsingHshMap

import (
	"fmt"
	"github.com/raralabs/canal/core/message"
	"github.com/raralabs/canal/core/message/content"
	"github.com/raralabs/canal/utils/regparser"
	"regexp"
	"strings"
)


// Represents the type of join Inner, LeftOuter
type JoinType uint8

// These are the currently supported types
const (
	INNER     	JoinType = iota + 1 // For inner join
	LEFTOUTER
)

//interface to implement for different types of join
type joinInterface interface {
	//returns the type of join
	Type() JoinType

	//joins two stream instances
	Join(inStream1,inStream2 content.IContent) content.IContent

	//returns the joined streams of message
	//JoinedMsg()[]c

	//condition for the join
	Condition(query string)

	//get stream from 1st route
	ProcessStreamFirst(msg message.Msg)

	//get stream from 2nd route
	ProcessStreamSec(msg message.Msg)
	//
	////gives the right table
	//RightTable() []stream
}

type innerJoin struct{
	query 			string
	//joinCondition	map[string]string
	joinType   		JoinType
	hashTable  		*HashTable
	streamContainer []message.Msg
	//joinMu   		*sync.Mutex
	mergedContent 	[]content.IContent
}

func NewInnerJoin(query string)*innerJoin{
	return &innerJoin{query: query, hashTable: NewHashMap(),mergedContent: []content.IContent{}}
}

func(in *innerJoin)Type()JoinType{
	return in.joinType
}

func(in *innerJoin)Join(inStream1,inStream2 content.IContent){
	merged := mergeContent(inStream1,inStream2)
	in.mergedContent = append(in.mergedContent,merged)
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

func Start(){
	//dummy query example
	query := "SELECT * FROM Stream1 INNERJOIN Stream2 ON Stream1.age,Stream1.first_name,Stream1. last_name = Stream2.age,Stream2.full_name"
	//creates a new inner join
	newJoin := NewInnerJoin(query)

	//prepares query by extractiong the join keys and condition
	fieldsFromStream1,fieldsFromStream2 := newJoin.Condition(query)

	//channels for message stream 1 and 2 to mimick the messages from path1 and path2
	messageStream1 := make(chan content.IContent)
	messageStream2 := make(chan content.IContent)

	go createMsgForPath1(100,messageStream1)
	go createMsgForPath2(100,messageStream2)

	//get the messages from stream1 and hold them in hash map
	for msg := range messageStream1{
		var joinFieldsVal []interface{}
		for _,field := range fieldsFromStream1{

			joinFieldsVal= append(joinFieldsVal,msg.Values()[strings.TrimSpace(field)])
		}
		key := concatKeys(joinFieldsVal)
		newJoin.hashTable.Set(msg,key)
	}

	//get the message from stream2 and check for the match in the hashmap
	count:=0
	for msg := range messageStream2{
		var joinFieldsVal []interface{}
		for _,field := range fieldsFromStream2{

			joinFieldsVal= append(joinFieldsVal,msg.Values()[strings.TrimSpace(field)])
		}
		key := concatKeys(joinFieldsVal)
		result,ok:=newJoin.hashTable.Get(key)
		if ok{
			newJoin.Join(result.(content.IContent),msg)
		}else{
			count++
			//fmt.Println(count,"not matched")
		}

	}
	fmt.Println(newJoin.mergedContent,len(newJoin.mergedContent),"joined")
}



