package joinUsingHshMap

import (
	"github.com/raralabs/canal/core/message"
	"github.com/raralabs/canal/core/message/content"
	"github.com/raralabs/canal/core/pipeline"
	"strings"
)

type JoinSubType uint8

const (
	INN			JoinSubType = iota+1
	FULLOUTER
	LEFTOUTER
	RIGHTOUTER
)
type outerJoin struct{
	joinType   		JoinType
	subType		JoinSubType
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


func NewOuterJoin(strategy JoinStrategy,firstPath,secondPath string,selectFields []string,subType JoinSubType)*outerJoin{
	switch strategy {
	case HASH://join using hash table
		return &outerJoin{hashTable: NewHashMap(),
			mergedContent: []content.IContent{},JoinStrategy:HASH,subType: subType,
			firstPath: firstPath,secondPath: secondPath,
			selectFields: selectFields}
	case TABLE://join using temporal table
		return &outerJoin{hashTable: NewHashMap(),
			mergedContent: []content.IContent{},JoinStrategy: TABLE,subType: subType,
			firstPath: firstPath,secondPath: secondPath,
			selectFields: selectFields}
	default:
		return &outerJoin{hashTable: NewHashMap(),
			mergedContent: []content.IContent{},JoinStrategy: HASH,subType: subType,
			firstPath: firstPath,secondPath: secondPath,
			selectFields: selectFields}
	}
}

func(oj *outerJoin)Type()JoinType{
	return oj.joinType
}

func(oj *outerJoin) mergeContent(inStream1,inStream2 content.IContent)content.IContent{
	dataTypeTracker1 := inStream1.Types()
	dataTypeTracker2 := inStream2.Types()
	messageContent1 := inStream1.Values()
	messageContent2 := inStream2.Values()
	newMsgContent := content.New()

	if oj.selectFields[0]!="*" {
		for _, key := range oj.selectFields {
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

func(oj *outerJoin)Join(messagePod pipeline.MsgPod,fields1,fields2 []string,proc pipeline.IProcessorForExecutor)bool{
	//extract msg from the pod
	m := messagePod.Msg
	//checks if the message is from first path or not if yes insert into the hash table till
	//eof is obtained
	if oj.subType == RIGHTOUTER||oj.subType==FULLOUTER{
		if pipeline.MsgRouteParam(oj.firstPath) == messagePod.Route {
			//if streams from path2 are live
			if m.Content().Keys()[0] != "eof" {
				oj.ProcessStreamFirst(m.Content(), fields1)
			}else{
				if oj.subType==FULLOUTER{
					oj.mergeLock = true
				}
			}
			//if stream is from first path then insert into the array till incoming msg from
			//path1 is complete

		} else if pipeline.MsgRouteParam(oj.secondPath) == messagePod.Route {
			if m.Content().Keys()[0] != "eof" {
				oj.secondContainer = append(oj.secondContainer, m.Content())
			} else { //after eof is obtained set the mergelock to true for merging
				oj.mergeLock = true
			}
		}
	}else if oj.subType==LEFTOUTER{
		if pipeline.MsgRouteParam(oj.secondPath) == messagePod.Route {
			//if streams from path2 are live
			if m.Content().Keys()[0] != "eof" {
				oj.ProcessStreamFirst(m.Content(), fields2)
			}
			//if stream is from first path then insert into the array till incoming msg from
			//path1 is complete
		} else if pipeline.MsgRouteParam(oj.firstPath) == messagePod.Route {
			if m.Content().Keys()[0] != "eof" {
				oj.secondContainer = append(oj.secondContainer, m.Content())
			} else { //after eof is obtained set the mergelock to true for merging
				oj.mergeLock = true
			}
		}
	}
	//var previous_content content.IContent = nil
	//if mergelock is true start merging the streams
	if oj.mergeLock ==true{
		for _,msg := range oj.secondContainer {
			result,ok := oj.ProcessStreamSec(msg,fields1)
			switch oj.subType{
			case LEFTOUTER:
				if ok{
					merged := oj.mergeContent(msg,result.(content.IContent))
					proc.Result(messagePod.Msg,merged,nil)
				}else{
					msgCont := content.New()
					merged := oj.mergeContent(msg,msgCont)
					proc.Result(messagePod.Msg,merged,nil)
				}
			case RIGHTOUTER:
				if ok{
					merged := oj.mergeContent(result.(content.IContent),msg)
					//previous_content = merged
					proc.Result(messagePod.Msg,merged,nil)
				}else {
					msgCont := content.New()
					merged := oj.mergeContent(msgCont, msg)
					proc.Result(messagePod.Msg, merged, nil)
				}
			default:
				if ok{
					merged := oj.mergeContent(result.(content.IContent),msg)
					proc.Result(messagePod.Msg,merged,nil)
				}else{
					msgCont := content.New()
					merged := oj.mergeContent(msgCont,msg)
					proc.Result(messagePod.Msg,merged,nil)

				}
			}
		if oj.subType == FULLOUTER {
			msgChannel := make(chan content.IContent)
			//msgCont := content.New()
			go oj.hashTable.iterate(msgChannel)
			//for msg := range msgChannel {
			//	merged := oj.mergeContent(msgCont,msg)
			//	proc.Result(messagePod.Msg,merged,nil)
			//}
		}
		}
		oj.secondContainer = nil
	}
	return true

}

func(oj *outerJoin)ProcessStreamFirst(msg content.IContent,fieldsFromStream1 []string){
	if oj.JoinStrategy == HASH{
		var joinFieldsVal []interface{}
		for _,field := range fieldsFromStream1{
			joinFieldsVal= append(joinFieldsVal,msg.Values()[strings.TrimSpace(field)])
		}
		key := concatKeys(joinFieldsVal)
		oj.hashTable.Set(msg,key)
	}
}
func(oj *outerJoin)ProcessStreamSec(msg content.IContent,fieldsFromStream2 []string)(interface{},bool){
	if oj.JoinStrategy == HASH{
		var joinFieldsVal []interface{}

		for _,field := range fieldsFromStream2{
			joinFieldsVal= append(joinFieldsVal,msg.Values()[strings.TrimSpace(field)])
		}

		key := concatKeys(joinFieldsVal)
		result,ok:= oj.hashTable.Get(key)
		return result,ok
	}
	return nil,false
}







