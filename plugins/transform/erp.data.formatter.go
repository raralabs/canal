package transform

import (
	"encoding/json"
	"fmt"
	"github.com/raralabs/canal/core/message"
	"github.com/raralabs/canal/core/message/content"
	"github.com/raralabs/canal/core/pipeline"
	"github.com/raralabs/pg-service-client/handlers/models"
	"log"
)


type erpDataFormatter struct{
	name    		string //name of the processor

}
func NewErpDataGetter (name string)*erpDataFormatter{
	return &erpDataFormatter{name:name}
}


//executes the process
func (dp *erpDataFormatter)Execute(messagePod pipeline.MsgPod, proc pipeline.IProcessorForExecutor)bool{
	var writeRequest models.WriteRequest
	var writeRequestContent *models.WriteRequestContent
	//var sentData data
	msg:= messagePod.Msg
	newContent := content.New()
	byteContent := msg.Values()["msg"]
	if err:=json.Unmarshal(byteContent.([]byte),&writeRequest);err!=nil{
		log.Panic("Could not unmarshall data")
	}
	newContent.Add("RequestID",content.NewFieldValue(writeRequest.RequestID,content.STRING))
	newContent.Add("AuthIdentity",content.NewFieldValue(writeRequest.AuthIdentity,content.STRING))
	newContent.Add("SourceData",content.NewFieldValue(writeRequest.SourceData,content.STRING))
	writeRequestContent = writeRequest.Content
	newContent.Add("Namespace",content.NewFieldValue(writeRequestContent.Namespace,content.STRING))
	newContent.Add("Collection",content.NewFieldValue(writeRequestContent.Collection,content.STRING))
	newContent.Add("SchemaID",content.NewFieldValue(writeRequestContent.SchemaID,content.STRING))
	newContent.Add("Data",content.NewFieldValue(writeRequestContent.Data.(map[string]interface{})["value"],content.INT))
	fmt.Println("newcontent",newContent)
	proc.Result(msg, newContent, nil)
	return false
}
//after the join processor is complete insert eof and closed it
func (dp *erpDataFormatter) done(m message.Msg, proc pipeline.IProcessorForExecutor) {
	// Send eof if done
	msgContents := content.New()
	msgContents = msgContents.Add("eof", content.NewFieldValue(true, content.BOOL))
	proc.Result(m, msgContents, nil)
	proc.Done()
}

func (dp *erpDataFormatter) ExecutorType() pipeline.ExecutorType {
	return pipeline.TRANSFORM
}


func (dp *erpDataFormatter) HasLocalState() bool {
	return false
}

func (dp *erpDataFormatter) SetName(name string) {
	dp.name = name
}

func (dp *erpDataFormatter) Name() string {
	return dp.name
}

