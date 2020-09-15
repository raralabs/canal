package message

import (
	"github.com/raralabs/canal/core/message/content"
	"github.com/stretchr/testify/assert"
	"testing"
)

//Test
// -NewFactory
func TestNewFactory(t *testing.T) {
	type testCase struct{
		testName 	string
		pipelineId 	uint32
		stageId	   	uint32
		processorId uint32
	}

	tests :=[]testCase{
		{"test1",1,2,3},
		{"test2",10,20,30},
	}
	for _,test := range tests {
		factory := NewFactory(test.pipelineId,test.stageId,test.processorId)
		assert.Equal(t,test.pipelineId,factory.pipelineId,"PipelineId doesn't match for test %s:",test.testName)
		assert.Equal(t,test.processorId,factory.processorId,"ProcessorId doesn't match for test %s:",test.testName)
		assert.Equal(t,test.stageId,factory.stageId,"StageId doesn't match for test %s:",test.testName)
		assert.Equal(t,uint64(0),factory.HWM)

	}
}

//Test
//	-NewExecuteRoot

func TestFactory_NewExecuteRoot(t *testing.T){
	type testCase struct{
		testName		string
		msgContent 		content.IContent
		trace			bool
	}
	tests := []testCase{
		{"empty msgContent with trace disabled",content.New(),false},
		{"empty msgContent with trace enabled", content.New(),true},
		{"msgContent with trace enabled",content.New().Add("msg",
			content.MsgFieldValue{10,content.INT}),
			true},
		{"msgContent with trace disabled",content.New().Add("msg",
			content.MsgFieldValue{10,content.INT}),
			false},
	}
	factory := NewFactory(1,1,2)
	for _,tst := range tests{
		msg := factory.NewExecuteRoot(tst.msgContent,tst.trace)
		assert.Equal(t,factory.pipelineId,msg.pipelineId,"Pipeline Id must be equal in test %s:",tst.testName)
		assert.Equal(t,factory.stageId,msg.stageId,"StageId must be equal in test %s:",tst.testName)
		assert.Equal(t,factory.processorId,msg.processorId,"Processor Id must be equal in test %s:",tst.testName)
		assert.Equal(t,EXECUTE,msg.msgType,"Message Type doesn't match for test %s:",tst.testName)
		assert.Equal(t,tst.trace,msg.trace.enabled,"Pipeline Id must be equal in test %s:",tst.testName)
		assert.Equal(t,tst.msgContent,msg.msgContent,"Contents must be equal in test %s:",tst.testName)


	}

}
//Test
// -Factory_NewExecute

func TestFactory_NewExecute(t *testing.T){

	type testCase struct{
		testName		string
		msgContent 		content.IContent
		prevContent		content.IContent
		srcMsg 			Msg
	}

	tests := []testCase{
		{"empty msgContent,empty prevContent and SrcMsg ",content.New(),content.New(),Msg{}},
		{"empty msgContent proper PrevContent", content.New(),content.New().Add("hello",
			content.MsgFieldValue{1,content.INT}),Msg{}},
		{" Proper msgontent empty PrevContent and SrcMsg", content.New().Add("hello",
			content.MsgFieldValue{1,content.INT}),content.New(),Msg{}},
		{" Proper msgontent empty PrevContent and SrcMsg", content.New().Add("hello",content.MsgFieldValue{1,content.INT}),
			content.New().Add("hello",content.MsgFieldValue{"hello",content.STRING}),Msg{}},
		{" Proper msgontent empty PrevContent and SrcMsg", content.New().Add("hello",content.MsgFieldValue{1,content.INT}),
			content.New().Add("hello",content.MsgFieldValue{"hello",content.STRING}),Msg{id:1,processorId: 2,stageId: 3}},
	}
	factory := NewFactory(1,1,2)
	for _,tst := range tests{
		msg := factory.NewExecute(tst.srcMsg,tst.msgContent,tst.prevContent)
		assert.Equal(t,factory.pipelineId,msg.pipelineId,"Pipeline Id must be equal in test %s:",tst.testName)
		assert.Equal(t,factory.stageId,msg.stageId,"StageId must be equal in test %s:",tst.testName)
		assert.Equal(t,factory.processorId,msg.processorId,"Processor Id must be equal in test %s:",tst.testName)
		assert.Equal(t,EXECUTE,msg.msgType,"Message Type doesn't match for test %s:",tst.testName)
		assert.Equal(t,tst.prevContent,msg.prevContent,"Pipeline Id must be equal in test %s:",tst.testName)
		assert.Equal(t,tst.msgContent,msg.msgContent,"Contents must be equal in test %s:",tst.testName)
		assert.Equal(t,tst.srcMsg.processorId,msg.srcProcessorId,"Id must be equal in test %s:",tst.testName)
		assert.Equal(t,tst.srcMsg.stageId,msg.srcStageId,"stage Id of src  must be equal in test %s:",tst.testName)
		assert.Equal(t,tst.srcMsg.id,msg.srcMessageId,"message id of src must be equal in test %s:",tst.testName)
	}

}
//Test
// -TestFactory_NewError

func TestFactory_NewError(t *testing.T) {
	type testCase struct{
		testName		string
		srcMsg 			*Msg
		code 			uint8
		mes				string
	}
	tests := []testCase{
		{"empty srcmsg with error 1",&Msg{},1,"Resource not found"},
		{"srcmsg with error 2 ", &Msg{id:1,processorId: 2,stageId: 3},2,"Server Error"},
		{"srcmsg with error 3", &Msg{id:1,processorId: 2,stageId: 3},3,"fatal error"},
	}

	factory := NewFactory(1,1,2)
	for _,tst := range tests{
		msg := factory.NewError(tst.srcMsg,tst.code,tst.mes)
		assert.Equal(t,factory.pipelineId,msg.pipelineId,"Pipeline Id must be equal in test %s:",tst.testName)
		assert.Equal(t,factory.stageId,msg.stageId,"StageId must be equal in test %s:",tst.testName)
		assert.Equal(t,factory.processorId,msg.processorId,"Processor Id must be equal in test %s:",tst.testName)
		assert.Equal(t,ERROR,msg.msgType,"Message Type doesn't match for test %s:",tst.testName)
		for key,value := range msg.msgContent.Values(){
			if key == "code" {
				assert.Equal(t,tst.code,value,"code must match" )
			}
			if key =="text"{
				assert.Equal(t,tst.mes,value,"error des must match")
			}
		}
		assert.Equal(t,tst.srcMsg.processorId,msg.srcProcessorId,"Id must be equal in test %s:",tst.testName)
		assert.Equal(t,tst.srcMsg.stageId,msg.srcStageId,"stage Id of src  must be equal in test %s:",tst.testName)
		assert.Equal(t,tst.srcMsg.id,msg.srcMessageId,"message id of src must be equal in test %s:",tst.testName)
	}
}



