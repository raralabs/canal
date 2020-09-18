package joinUsingHshMap

import (
	"github.com/raralabs/canal/core/message/content"
)

//join the content of the msg1 and msg2
func mergeContent (msgContent1,msgContent2 content.IContent)content.IContent{
	dataTypeTracker := make(map[interface{}]content.FieldValueType)
	newMsgContent := content.New()
	for key,contentType := range msgContent1.Types(){
		dataTypeTracker[key]=contentType
	}
	for key,contentType := range msgContent2.Types(){
		dataTypeTracker[key]=contentType
	}
	for key,value := range msgContent1.Values(){
		newMsgContent.Add(key,content.NewFieldValue(value,dataTypeTracker[key]))
	}
	for key,value := range msgContent2.Values(){
		newMsgContent.Add(key,content.NewFieldValue(value,dataTypeTracker[key]))
	}

	return newMsgContent
}
