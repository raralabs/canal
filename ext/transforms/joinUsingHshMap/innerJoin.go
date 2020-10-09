package joinUsingHshMap

import (
	"github.com/raralabs/canal/core/pipeline"
	"fmt"
	"github.com/raralabs/canal/core/message"
	"github.com/raralabs/canal/core/message/content"
	"strings"
)

type innerJoin struct{
	joinType   		JoinType
	hashTable  		*HashTable
	streamContainer []message.Msg
	mergedContent 	[]content.IContent
	JoinStrategy	JoinStrategy
	firstPath		string
	secondPath		string
	mergeLock 		bool
	secondContainer []content.IContent
	selectFields 	[]string
}


func NewInnerJoin(strategy JoinStrategy,firstPath,secondPath string,selectFields []string)*innerJoin{
	switch strategy {
	case HASH://join using hash table
		return &innerJoin{hashTable: NewHashMap(),
			mergedContent: []content.IContent{},
			JoinStrategy:HASH,firstPath: firstPath,
			secondPath: secondPath,selectFields: selectFields}
	case TABLE://join using temporal table
		return &innerJoin{hashTable: NewHashMap(),
			mergedContent: []content.IContent{},
			JoinStrategy: TABLE,firstPath: firstPath,
			secondPath: secondPath,selectFields: selectFields}
	default:
		return &innerJoin{hashTable: NewHashMap(),
			mergedContent: []content.IContent{},
			JoinStrategy: HASH,firstPath: firstPath,
			secondPath: secondPath,selectFields: selectFields}
	}
}

func(in *innerJoin)Type()JoinType{
	return in.joinType
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

func(in *innerJoin)Join(messagePod pipeline.MsgPod,fields1,fields2 []string,proc pipeline.IProcessorForExecutor)bool{
	//extract msg from the pod
	m := messagePod.Msg
	//checks if the message is from first path or not if yes insert into the hash table till
	//eof is obtained
	if pipeline.MsgRouteParam(in.firstPath) == messagePod.Route{

		//if streams from path1 are live
		if m.Content().Keys()[0] != "eof" {
			in.ProcessStreamFirst(m.Content(), fields1)
		}else{
			in.mergeLock = true
		}
		//if stream is from second path then insert into the array till incoming msg from
		//path1 is complete
	}else if pipeline.MsgRouteParam(in.secondPath) == messagePod.Route{
		if m.Content().Keys()[0] !="eof" {
			in.secondContainer = append(in.secondContainer, m.Content())
		}
	}
	//if mergelock is true start merging the streams
	if in.mergeLock == true{
		for _,msg := range in.secondContainer {
			result,ok := in.ProcessStreamSec(msg,fields2)
			if ok{
				merged := in.mergeContent(msg,result.(content.IContent))
				proc.Result(messagePod.Msg,merged,nil)
				continue
			}
		}
		in.secondContainer = nil
	}
	return true
}

func(in *innerJoin) mergeContent(inStream1,inStream2 content.IContent)content.IContent{
	dataTypeTracker1 := inStream1.Types()
	dataTypeTracker2 := inStream2.Types()
	messageContent1 := inStream1.Values()
	messageContent2 := inStream2.Values()
	newMsgContent := content.New()
	if in.selectFields[0]!="*" {
		for _, key := range in.selectFields {
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





