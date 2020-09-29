package joinUsingHshMap

import (
	"github.com/raralabs/canal/core/message/content"
	"github.com/raralabs/canal/core/pipeline"
	"strings"
	"sync"
)

type JoinSubType uint8

const (
	INN			JoinSubType = iota+1
	FULLOUTER
	LEFTOUTER
	RIGHTOUTER
)
type outerJoin struct{
	joinType   		JoinType //holds the type of join
	subType		JoinSubType // sub type of outer join like left right full
	hashTable  		*HashTable // hash table to hold the information from one of the path
	JoinStrategy	JoinStrategy // strategy of join like hash, window etc
	firstPath		pipeline.MsgRouteParam // name of the first path
	secondPath		pipeline.MsgRouteParam // name of the second path
	mergeLock 		bool                   // lock to start merging
	mutex			sync.Mutex            // mutex to lock certain process
	secondContainer []content.IContent	  //container holds the streams from second path
	selectFields 	[]string			// select fields from the query
	firstPathEnd	bool				//true if the incoming msg from first path are complete
	secondPathEnd	bool				// true if the incoming msg from the second path are complete
}


func NewOuterJoin(strategy JoinStrategy,firstPath,secondPath string,selectFields []string,subType JoinSubType)*outerJoin{
	switch strategy {
	case HASH://join using hash table
		return &outerJoin{hashTable: NewHashMap(),
			JoinStrategy:HASH,subType: subType,
			firstPath: pipeline.MsgRouteParam(firstPath),secondPath: pipeline.MsgRouteParam(secondPath),
			selectFields: selectFields}
	case TABLE://join using temporal table
		return &outerJoin{hashTable: NewHashMap(),
			JoinStrategy: TABLE,subType: subType,
			firstPath: pipeline.MsgRouteParam(firstPath),secondPath: pipeline.MsgRouteParam(secondPath),
			selectFields: selectFields}
	default:
		return &outerJoin{hashTable: NewHashMap(),
			JoinStrategy: HASH,subType: subType,
			firstPath: pipeline.MsgRouteParam(firstPath),secondPath: pipeline.MsgRouteParam(secondPath),
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
	return newMsgContent
}

func(oj *outerJoin)Join(messagePod pipeline.MsgPod,fields1,fields2 []string,proc pipeline.IProcessorForExecutor)bool{
	//extract msg from the pod
	m := messagePod.Msg
	//checks if the message is from first path or not if yes insert into the hash table till
	//eof is obtained
	if oj.subType == RIGHTOUTER||oj.subType==FULLOUTER{
		if pipeline.MsgRouteParam(oj.firstPath) == messagePod.Route {
			//if streams from path1 are live
			if m.Content().Keys()[0] != "eof" {
				oj.ProcessStreamFirst(m.Content(), fields1)
			}else{
				oj.firstPathEnd = true
				oj.mergeLock = true
			}
			//if stream is from first path then insert into the array till incoming msg from
			//path1 is complete
		} else if pipeline.MsgRouteParam(oj.secondPath) == messagePod.Route{
			if m.Content().Keys()[0] != "eof" {
				oj.secondContainer = append(oj.secondContainer, m.Content())
			} else { //after eof is obtained set the mergelock to true for merging
				oj.secondPathEnd = true

			}
		}
	}else if oj.subType==LEFTOUTER{
		if pipeline.MsgRouteParam(oj.secondPath) == messagePod.Route {
			//if streams from path2 are live
			if m.Content().Keys()[0] != "eof" {
				oj.ProcessStreamFirst(m.Content(), fields2)
			}else{
				oj.secondPathEnd = true
				oj.mergeLock = true
			}
			//if stream is from first path then insert into the array till incoming msg from
			//path1 is complete
		} else if pipeline.MsgRouteParam(oj.secondPath) == messagePod.Route {
			if m.Content().Keys()[0] != "eof" {
				oj.secondContainer = append(oj.secondContainer, m.Content())
			} else { //after eof is obtained set the mergelock to true for merging
				oj.firstPathEnd= true

			}
		}

	}
	//var previous_content content.IContent = nil
	//if mergelock is true start merging the streams
	if oj.mergeLock ==true{
		oj.mutex.Lock() //lock the merging process so as to avoid race conditions
		for _,msg := range oj.secondContainer { //read the container and merge according to the type of joins
			if oj.subType == LEFTOUTER {
				result, ok := oj.ProcessStreamSec(msg, fields1)
				if ok {
					merged := oj.mergeContent(msg, result.(content.IContent))
					proc.Result(messagePod.Msg, merged, nil)
				} else {
					msgCont := content.New()
					merged := oj.mergeContent(msg, msgCont)
					proc.Result(messagePod.Msg, merged, nil)
				}

				oj.secondContainer = nil //clean containers to avoid msg duplications
			} else if oj.subType == RIGHTOUTER {
				result, ok := oj.ProcessStreamSec(msg, fields2)
				if ok {
					merged := oj.mergeContent(result.(content.IContent), msg)
					//previous_content = merged
					proc.Result(messagePod.Msg, merged, nil)
				} else {
					msgCont := content.New()
					merged := oj.mergeContent(msgCont, msg)
					proc.Result(messagePod.Msg, merged, nil)
				}
				oj.secondContainer = nil //clean container to avoid msg duplications
			} else {
				result, ok := oj.ProcessStreamSec(msg, fields2)
				if ok { //if found on the hash map merge returned result and current msg
					merged := oj.mergeContent(result.(content.IContent), msg)
					proc.Result(messagePod.Msg, merged, nil)
				} else {
					msgCont := content.New()
					merged := oj.mergeContent(msgCont, msg)
					proc.Result(messagePod.Msg, merged, nil)
				}
				oj.secondContainer = nil //after merging clean container to avoid msg duplications
			}

		}
		//incase of full outer joins perform this additional steps to merge the left side table
		if oj.subType == FULLOUTER && oj.secondPathEnd==true{
			linkedList := oj.hashTable.data
			for _,table := range(linkedList){
				if table!=nil {
					node :=table.Head
					for node!= nil{
						merged := oj.mergeContent(node.Data.(listData).value.(content.IContent),content.New())
						proc.Result(messagePod.Msg,merged,nil)
						node = node.Next
					}
				}
			}
			}

		oj.secondContainer = nil
		oj.mutex.Unlock()
	}

	return true

}

//the data from first streams are inserted into the hash table
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
//check if the data from second streams matches that of the hash table
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







