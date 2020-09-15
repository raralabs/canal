package message

import (
	//"bytes"
	"encoding/gob"
	"github.com/raralabs/canal/core/message/content"
	"github.com/stretchr/testify/assert"
	"reflect"
	"testing"
)

func init() {
	// Register the message attribute to gob
	gob.Register(content.MsgFieldValue{})
}

//Tests:
//	-SetField
func TestMsg_SetField(t *testing.T) {
	type args struct {
		key      string
		msgValue content.MsgFieldValue
	}
	testCases := []struct {
		name string
		prev content.MsgFieldValue
		args args
		want content.MsgFieldValue
	}{
		{"testEmptyMsg",
			content.MsgFieldValue{"", content.STRING},
			args{"test", content.MsgFieldValue{"", content.STRING}},
			content.MsgFieldValue{"", content.STRING},
		},
		{"testNewProperMsgAdd",
			content.MsgFieldValue{"", content.STRING},
			args{"test", content.MsgFieldValue{"key", content.STRING}},
			content.MsgFieldValue{"key", content.STRING},
		},
		{"testForNumbers",
			content.MsgFieldValue{1, content.INT},
			args{"test", content.MsgFieldValue{1, content.INT}},
			content.MsgFieldValue{1, content.INT},
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			msg := &Msg{msgContent: content.New()}
			msg.msgContent.Add("test", test.prev)
			want := msg
			if got := msg.SetField(test.args.key, test.args.msgValue); !reflect.DeepEqual(got, want) {

				t.Errorf("Msg.SetField() = %v, want %v in %s", got, want, test.name)
			}
		})

	}
}

//Tests:
//	- Msg_Id
func TestMsg_Id(t *testing.T) {
	expectedIds := []uint64{1, 99, 15}
	messages := []*Msg{
		{id: 1},
		{id: 99},
		{id: 15}}
	for idx, msg := range messages {
		assert.Equal(t, expectedIds[idx], msg.Id(), "Id didn't match")

	}
}

//Tests:
//	- Message_AsBytes
func TestMsg_AsBytes(t *testing.T) {
	type message struct {
		testName string
		msg      *Msg
	}
	var cont content.IContent
	cont = content.New()
	cont.Add("key", content.NewFieldValue("hello", content.STRING))
	messages := []message{
		{"complete EXECUTE msg with empty content", &Msg{id: 1, processorId: 1,
			srcMessageId: 2, stageId: 10, srcProcessorId: 1, srcStageId: 3,
			msgType: EXECUTE, msgContent: content.New(), prevContent: content.New(),
			trace: trace{false, []tracePath{}},
		}},
		{"complete CONTROL msg with empty content", &Msg{id: 1, processorId: 1,
			srcMessageId: 2, stageId: 10, srcProcessorId: 1, srcStageId: 3,
			msgType: CONTROL, msgContent: content.New(), prevContent: content.New(),
			trace: trace{false, []tracePath{}},
		}},
		{"complete ERROR msg with content", &Msg{id: 1, processorId: 1,
			srcMessageId: 2, stageId: 10, srcProcessorId: 1, srcStageId: 3,
			msgType: ERROR, msgContent: cont, prevContent: content.New(),
			trace: trace{false, []tracePath{}},
		}},
		{"complete ERROR msg with prevcontent trace enabled", &Msg{id: 1, processorId: 1,
			srcMessageId: 2, stageId: 10, srcProcessorId: 1, srcStageId: 3,
			msgType: ERROR, msgContent: content.New(), prevContent: cont,
			trace: trace{false, []tracePath{}},
		}},
	}
	for _, testMsg := range messages {

		if _, err := testMsg.msg.AsBytes(); err != nil {
			t.Errorf("could not encode message in test %s", testMsg.testName)
		}
	}

}

func TestNewFromBytes(t *testing.T) {
	var cont content.IContent
	cont = content.New()
	cont.Add("key", content.NewFieldValue("hello", content.STRING))
	tests := []struct {
		testName string
		inAndOut *Msg
		wantErr  bool
	}{
		{"complete EXECUTE msg with nil content", &Msg{id: 1, processorId: 1,
			srcMessageId: 2, stageId: 10, srcProcessorId: 1, srcStageId: 3,
			msgType: EXECUTE, prevContent: content.New(),
			trace: trace{false, []tracePath(nil)},
		}, false},
		{"complete CONTROL msg with empty content", &Msg{id: 1, processorId: 1,
			srcMessageId: 2, stageId: 10, srcProcessorId: 1, srcStageId: 3,
			msgType: CONTROL, msgContent: content.New(), prevContent: content.New(),
			trace: trace{false, []tracePath(nil)},
		}, false},
		{"complete ERROR msg with content", &Msg{id: 1, processorId: 1,
			srcMessageId: 2, stageId: 10, srcProcessorId: 1, srcStageId: 3,
			msgType: ERROR, msgContent: cont, prevContent: content.New().Add("key2", content.NewFieldValue("hello", content.STRING)),
			trace: trace{false, []tracePath(nil)},
		}, false},
		{"complete ERROR msg with prevcontent trace enabled", &Msg{id: 1, processorId: 1,
			srcMessageId: 2, stageId: 10, srcProcessorId: 1, srcStageId: 3,
			msgType: ERROR, msgContent: content.New().Add("key2", content.NewFieldValue("hello", content.STRING)),
			prevContent: cont,
			trace:       trace{false, []tracePath(nil)},
		}, false},
		{"complete ERROR msg with prevcontent trace enabled", &Msg{id: 1, processorId: 1,
			srcMessageId: 2, stageId: 10, srcProcessorId: 1, srcStageId: 3,
			msgType: ERROR, msgContent: content.New().Add("key2", content.NewFieldValue("hello", content.STRING)),
			prevContent: nil,
			trace:  trace{false, []tracePath(nil)},
		}, false},
		{"complete ERROR msg with prevcontent trace enabled", &Msg{id: 1, processorId: 1,
			srcMessageId: 2, stageId: 10, srcProcessorId: 1, srcStageId: 3,
			msgType: ERROR, msgContent: nil,
			prevContent: cont,
			trace:       trace{false, []tracePath(nil)},
		}, false},
		{"complete ERROR msg with prevcontent trace enabled", &Msg{id: 1, processorId: 1,
			srcMessageId: 2, stageId: 10, srcProcessorId: 1, srcStageId: 3,
			msgType: ERROR, msgContent: nil,
			prevContent: nil,
			trace:       trace{false, []tracePath(nil)},
		}, false},
	}

	for _, tt := range tests {
		t.Run(tt.testName, func(t *testing.T) {
			// Encode the message to an array of bytes
			bts, err := tt.inAndOut.AsBytes()
			//Decode the array of bytes to message
			got, err := NewFromBytes(bts)

			if (err != nil) != tt.wantErr {
				t.Errorf("NewFromBytes() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(tt.inAndOut, got) {
				t.Errorf("NewFromBytes() = %v, want %v", got, tt.inAndOut)
			}

		})
	}
}

//Tests:
//content()
func TestMsg_Content(t *testing.T) {

	var msg = &Msg{msgContent: content.New()}
	keys := []string{"name", "roll", "msg"}
	msgValues := []content.MsgFieldValue{
		content.NewFieldValue("This is testing", content.STRING),
		content.NewFieldValue(12, content.INT),
		content.NewFieldValue("xyz", content.STRING),
	}
	want := map[string]interface{}{}
	t.Run("Content", func(t *testing.T) {
		for idx, msgValue := range msgValues {
			want[keys[idx]] = msgValue.Val
			msg.msgContent.Add(keys[idx], msgValue)
			if got := msg.msgContent; !reflect.DeepEqual(got.Values(), want) {
				t.Errorf("Msg.Values() = %v, want %v", got.Values(), want)
			}
		}
		assert.Equal(t, len(keys), len(msg.Content().Values()),
			"length must be equal to the number of item added")

	})

}

//Tests:
// -Msg_Values
func TestMsg_PrevContent(t *testing.T) {
	var msg = &Msg{prevContent: content.New()}
	keys := []string{"name", "roll", "msg"}
	msgValues := []content.MsgFieldValue{
		content.NewFieldValue("This is testing", content.STRING),
		content.NewFieldValue(12, content.INT),
		content.NewFieldValue("xyz", content.STRING),
	}
	want := map[string]interface{}{}
	t.Run("PrevContent", func(t *testing.T) {
		for idx, msgValue := range msgValues {
			want[keys[idx]] = msgValue.Val
			msg.prevContent.Add(keys[idx], msgValue)
			if got := msg.prevContent; !reflect.DeepEqual(got.Values(), want) {
				t.Errorf("Msg.Values() = %v, want %v", got.Values(), want)
			}
		}
		assert.Equal(t, len(keys), len(msg.PrevContent().Values()),
			"length must be equal to the number of item added")

	})

}

//Tests:
//	Message_IsControl
//	Message_IsExecute
//	Message_IsError
func TestMsg_MsgTypes(t *testing.T) {
	var msg *Msg
	testParams := []MsgType{
		CONTROL, EXECUTE, ERROR,
	}
	msg = &Msg{}
	for _, testParam := range testParams {
		msg.msgType = testParam
		switch testParam {
		case CONTROL:
			assert.Equal(t, true, msg.IsControl())
		case EXECUTE:
			assert.Equal(t, true, msg.IsExecute())
		case ERROR:
			assert.Equal(t, true, msg.IsError())
		}
	}
}

//Tests
//	-Message_Id
//	-Message_ProcessorId
//  -Message_StageId
func TestMsg_Ids(t *testing.T) {
	type tests struct {
		testName string
		message  *Msg
	}
	testMessages := []*tests{
		{"Empty msg", &Msg{}},
		{"only msg id", &Msg{id: 1}},
		{"msg with all valid ids", &Msg{id: 1, processorId: 1,
			srcMessageId: 2, stageId: 10, srcProcessorId: 1, srcStageId: 3}},
	}

	for _, testMsg := range testMessages {
		id := testMsg.message.id
		assert.Equal(t, id, testMsg.message.Id(), "Assigned Id must match retrieved Id in %s in test:", testMsg.testName)
		processorId := testMsg.message.processorId
		assert.Equal(t, processorId, testMsg.message.ProcessorId(), "Assigned Id must match retrieved Id %s in test:", testMsg.testName)
		stageId := testMsg.message.stageId
		assert.Equal(t, stageId, testMsg.message.StageId(), "Assigned Id must match retrieved Id in %s in test:", testMsg.testName)
	}
}

//Tests:
// -Msg_String
func TestMsg_String(t *testing.T) {
	msg := &Msg{id: 1, processorId: 1, srcMessageId: 2, stageId: 10, srcProcessorId: 1, srcStageId: 3,
		msgType: ERROR, msgContent: nil, prevContent: content.New(),
		trace: trace{false, []tracePath(nil)}}
	strMessage := msg.String()
	assert.Equal(t, "string", reflect.TypeOf(strMessage).String(), "DataType mismatch")
}

func TestMsg_NewError(t *testing.T) {
	type errorAttr struct {
		pipelineId  uint32
		stageId     uint32
		processorId uint32
		code        uint8
		text        string
	}
	type tests struct {
		testName   string
		testParams errorAttr
	}
	testCases := []*tests{
		{"Resource Error", errorAttr{1, 2, 3, 1, "resource not found"}},
		{"Server Error", errorAttr{4, 5, 3, 2, "server not found"}},
	}
	for _, test := range testCases {
		errMsg := NewError(test.testParams.pipelineId, test.testParams.stageId,
			test.testParams.processorId, test.testParams.code, test.testParams.text)
		assert.IsType(t, Msg{}, errMsg, "should of type Msg")
		assert.Equal(t, test.testParams.pipelineId, errMsg.pipelineId)
		assert.Equal(t, test.testParams.stageId, errMsg.StageId())
		assert.Equal(t, test.testParams.processorId, errMsg.ProcessorId())
		wantContent := make(map[string]interface{})
		wantContent["text"] = test.testParams.text
		wantContent["code"] = test.testParams.code
		assert.Equal(t, wantContent, errMsg.msgContent.Values())
	}
}
func TestMsg_SetPrevContent(t *testing.T) {
	type tests struct {
		testName string
		content  content.IContent
	}
	var msg *Msg
	msg = &Msg{id: 1}
	newContent := content.New()
	testMessages := []*tests{
		{"Empty msg", newContent},
		{"only msg id", newContent.Add("greetings", content.NewFieldValue("hello", content.STRING))},
		{"only msg id", newContent.Add("number", content.NewFieldValue(1, content.INT))},
	}
	for _, test := range testMessages {
		msg.SetPrevContent(test.content)
		assert.Equal(t, test.content, msg.prevContent, "Inserted content doesn't match with the retrieved content")
	}

}

func TestMsg_Trace(t *testing.T) {
	tests := []struct {
		testName     string
		inputMessage *Msg
		want         *trace
	}{
		{"trace with default flag", &Msg{}, &trace{}},
		{"trace disabled", &Msg{trace: trace{false, []tracePath(nil)}}, &trace{false, []tracePath(nil)}},
		{"trace enabled with empty tracePath", &Msg{trace: trace{true, []tracePath(nil)}}, &trace{true, []tracePath(nil)}},
		{"trace enabled with tracePath", &Msg{trace: trace{false, []tracePath{{1, 2, 3}}}}, &trace{false, []tracePath{{1, 2, 3}}}},
	}

	for _, test := range tests {
		t.Run(test.testName, func(t *testing.T) {
			if !reflect.DeepEqual(test.inputMessage.Trace(), test.want) {
				t.Errorf("NewFromBytes() = %v, want %v in test: %s", test.inputMessage.Trace(), test.want, test.testName)
			}
		})
	}

}

//Tests:
// -MsgContent_Types()
func TestMsgContent_Types(t *testing.T) {
	tests := []struct {
		testName string
		inputMsg *Msg
		want     string
	}{
		{"msg with empty content", &Msg{msgContent: content.New(), prevContent: content.New()}, "nil"},

		{"msg with int content", &Msg{id: 10, msgType: CONTROL,
			msgContent: content.New().Add("key1", content.NewFieldValue(10, content.INT))}, "int",
		},

		{"msg with string content", &Msg{id: 1, processorId: 1, msgType: EXECUTE,
			msgContent:  content.New().Add("key3", content.NewFieldValue("hello", content.STRING)),
			prevContent: content.New()}, "str",
		},
	}
	for _, test := range tests {
		t.Run(test.testName, func(t *testing.T) {
			typeOfMsgContent := test.inputMsg.Types()
			for _, value := range typeOfMsgContent {
				assert.Equal(t, test.want, value.String(), "DataTypes didn't match")
			}
		})
	}
}

//Tests:
// -Msg_Values
func TestMsg_Values(t *testing.T) {
	tests := []struct {
		testName string
		in       *Msg
		want     map[string]interface{}
	}{
		{"msg with empty content",
			&Msg{msgContent: content.New(), prevContent: content.New()},
			map[string]interface{}{},
		},

		{"msg with int content",
			&Msg{msgContent: content.New().Add("key1", content.NewFieldValue(10, content.INT))},
			map[string]interface{}{"key1": 10},
		},

		{"msg with string content",
			&Msg{msgContent: content.New().Add("key2", content.NewFieldValue("hello", content.STRING)), prevContent: content.New()},
			map[string]interface{}{"key2": "hello"},
		},
	}
	for _, test := range tests {
		t.Run(test.testName, func(t *testing.T) {
			if got := test.in.Values(); !reflect.DeepEqual(got, test.want) {
				t.Errorf("Msg.Values() = %v, want %v", got, test.want)
			}
		})
	}
}
