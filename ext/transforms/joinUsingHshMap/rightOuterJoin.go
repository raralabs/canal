package joinUsingHshMap

import (
"github.com/raralabs/canal/core/message"
"github.com/raralabs/canal/core/message/content"
"github.com/raralabs/canal/core/pipeline"
"strings"
)

type rightOuterJoin struct{
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


func NewRightOuterJoin(strategy JoinStrategy,firstPath,secondPath string,selectFields []string)*rightOuterJoin{
	switch strategy {
	case HASH://join using hash table
		return &rightOuterJoin{hashTable: NewHashMap(),
			mergedContent: []content.IContent{},JoinStrategy:HASH,
			firstPath: firstPath,secondPath: secondPath,
			selectFields: selectFields}
	case TABLE://join using temporal table
		return &rightOuterJoin{hashTable: NewHashMap(),
			mergedContent: []content.IContent{},JoinStrategy: TABLE,
			firstPath: firstPath,secondPath: secondPath,
			selectFields: selectFields}
	default:
		return &rightOuterJoin{hashTable: NewHashMap(),
			mergedContent: []content.IContent{},JoinStrategy: HASH,
			firstPath: firstPath,secondPath: secondPath,
			selectFields: selectFields}
	}
}

func(roj *rightOuterJoin)Type()JoinType{
	return roj.joinType
}

func(roj *rightOuterJoin) mergeContent(inStream1,inStream2 content.IContent)content.IContent{
	dataTypeTracker1 := inStream1.Types()
	dataTypeTracker2 := inStream2.Types()
	messageContent1 := inStream1.Values()
	messageContent2 := inStream2.Values()
	newMsgContent := content.New()

	if roj.selectFields[0]!="*" {
		for _, key := range roj.selectFields {
			newMsgField1, ok1 := messageContent1[key]
			if ok1 {
				newMsgContent.Add(key, content.NewFieldValue(newMsgField1, dataTypeTracker1[key]))
			}
			newMsgField2, ok2 := messageContent2[key]
			if ok2 {
				newMsgContent.Add(key, content.NewFieldValue(newMsgField2, dataTypeTracker2[key]))
			}else if !(ok1||ok2){
				newMsgContent.Add(key,content.MsgFieldValue{})
			}
			//if !(ok1){
			//	pErr:= fmt.Sprintf("failed to get specified field,neither streams contains field: %s",key)
			//	panic(pErr)
			//}
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

func(roj *rightOuterJoin)Join(messagePod pipeline.MsgPod,fields1,fields2 []string,proc pipeline.IProcessorForExecutor)bool{
	//extract msg from the pod
	m := messagePod.Msg
	//checks if the message is from first path or not if yes insert into the hash table till
	//eof is obtained
	if pipeline.MsgRouteParam(roj.firstPath) == messagePod.Route {
		//if streams from path1 are live
		if m.Content().Keys()[0] != "eof" {
			roj.ProcessStreamFirst(m.Content(), fields1)
		}
		//if stream is from first path then insert into the array till incoming msg from
		//path1 is complete
	}else if pipeline.MsgRouteParam(roj.secondPath) == messagePod.Route{
		if m.Content().Keys()[0] !="eof" {
			roj.secondContainer = append(roj.secondContainer, m.Content())
		}else{//after eof is obtained set the mergelock to true for merging
			roj.mergeLock = true
		}
	}
	//var previous_content content.IContent = nil
	//if mergelock is true start merging the streams
	if roj.mergeLock ==true{
		for _,msg := range roj.secondContainer {
			result,ok := roj.ProcessStreamSec(msg,fields1)
			if ok{
				merged := roj.mergeContent(result.(content.IContent),msg)
				//previous_content = merged
				proc.Result(messagePod.Msg,merged,nil)
			}else{
				msgCont:=  content.New()
				merged := roj.mergeContent(msgCont,msg)
				proc.Result(messagePod.Msg,merged,nil)
			}
		}
		roj.secondContainer = nil
	}
	return true

}

func(roj *rightOuterJoin)ProcessStreamFirst(msg content.IContent,fieldsFromStream1 []string){
	if roj.JoinStrategy == HASH{
		var joinFieldsVal []interface{}
		for _,field := range fieldsFromStream1{
			joinFieldsVal= append(joinFieldsVal,msg.Values()[strings.TrimSpace(field)])
		}
		key := concatKeys(joinFieldsVal)
		roj.hashTable.Set(msg,key)
	}
}
func(roj *rightOuterJoin)ProcessStreamSec(msg content.IContent,fieldsFromStream2 []string)(interface{},bool){
	if roj.JoinStrategy == HASH{
		var joinFieldsVal []interface{}

		for _,field := range fieldsFromStream2{
			joinFieldsVal= append(joinFieldsVal,msg.Values()[strings.TrimSpace(field)])
		}

		key := concatKeys(joinFieldsVal)
		result,ok:= roj.hashTable.Get(key)
		return result,ok
	}
	return nil,false
}







