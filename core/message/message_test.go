package message

import (
	"bytes"
	"encoding/gob"
	"github.com/raralabs/canal/core/message/content"
	"github.com/stretchr/testify/assert"
	"testing"
)

func init() {
	// Register the message attribute to gob
	gob.Register(content.MsgFieldValue{})
	//gob.Register(cMsgFieldValue{})
}

//Tests:
//	-SetField
func TestMsg_SetField(t *testing.T) {
	var msg = &Msg{mcontent: content.New()}
	keys := []string{"name", "roll"}
	msgValues := []interface{}{
		content.NewFieldValue("xyz", content.STRING),
		content.NewFieldValue(12, content.INT),
	}

	t.Run("SetField", func(t *testing.T) {
		for idx, msgValue := range (msgValues) {
			msg = msg.SetField(keys[idx], msgValue.(content.MsgFieldValue))
		}
		msgkeys := msg.Content().Keys()
		assert.Equal(t,len(keys),len(msg.Content().Values()),
			"length must be equal to the number of item added")
		assert.ElementsMatch(t,keys, msgkeys, "keys must be Same")


	})

}

//Tests:
	//content()
func TestMsg_Content(t *testing.T) {
	var msg = &Msg{mcontent: content.New()}
	keys := []string{"name", "roll","msg"}
	msgValues := []content.MsgFieldValue{
		content.NewFieldValue("This is testing", content.STRING),
		content.NewFieldValue(12, content.INT),
		content.NewFieldValue("xyz", content.STRING),
	}
	t.Run("Content", func(t *testing.T) {
		for idx, msgValue := range (msgValues) {
			msg.mcontent.Add(keys[idx], msgValue)
		}
		assert.Equal(t,len(keys),len(msg.Content().Values()),
			"length must be equal to the number of item added")
	})
}

func NewFromBytes(bts []byte) (*Msg, error) {
	var m Msg
	var buf bytes.Buffer
	buf.Write(bts)
	decoder := gob.NewDecoder(&buf)
	err := decoder.Decode(&m)
	if err != nil {
		return nil, err
	}

	return &m, err
}


//Tests:
//	- NewFromBytes
//	- Message_AsBytes

func TestNewFromBytes(t *testing.T) {
	var msg *Msg


}

//func Test_NewMessageFromBytes(t *testing.T) {
//	//var msg *Msg
//	msgValue1 := content.NewFieldValue("xyz", content.STRING)
//	msgValue2 := content.NewFieldValue(12, content.INT)
//	msgContent := content.New()
//	msgContent.Add("name",msgValue1)
//	msgContent.Add("roll",msgValue2)
//
//	//msgMap.AddMessageValue("name", "Nischal", STRING)
//
//
//	//assert.Equal(t, STRING, msgMap["name"].ValueType(), "ValueType must be string")
//	//assert.Equal(t, "Nischal", msgMap["name"].Value(), "Name should be same")
//	//assert.Equal(t, STRING, msgMap["name"].ValueType(), "ValueType must be string")
//
//	tests := []struct {
//		name     string
//		inAndOut *Msg
//		wantErr  bool
//	}{
//		// TODO: add test cases.
//		//{"Empty Msg", &Msg{}, false},
//		//{"Only id", &Msg{id: 2}, false},
//		//{"id and type execute", &Msg{id: 2,mtype: EXECUTE}, false},
//		//{"id and type execute", &Msg{id: 2,mtype: CONTROL}, false},
//		//{"id and type execute", &Msg{id: 2,mtype: ERROR}, false},
//		//{"id,processorId and stageId", &Msg{id: 1, processorId: 1,stageId:1}, false},
//		//{"with ids and content",&Msg{id: 1, processorId: 99,stageId:100,mcontent: msgContent},false},
//		//{"Only content", &Msg{mcontent: msgContent}, false},
//		//{"Only content with trace enabled", &Msg{mcontent: msgContent,trace: trace{true,[]tracePath{}}}, false},
//		//{"complete message Control type", &Msg{id: 1,mtype:CONTROL,mcontent: msgContent,prevContent: msgContent,trace: trace{true,[]tracePath{}}}, false},
//		//{"complete message Execute type", &Msg{id: 1,mtype:EXECUTE,mcontent: msgContent,prevContent: msgContent,trace: trace{true,[]tracePath{}}}, false},
//		{"complete message Execute type", &Msg{id: 1,mtype:ERROR,mcontent: msgContent,prevContent: msgContent,trace: trace{true,[]tracePath{}}}, false},
//
//	}
//
//	for _, tt := range tests {
//		t.Run(tt.name, func(t *testing.T) {
//			// Encode the message to an array of bytes
//			bts, err := tt.inAndOut.AsBytes()
//
//			// Decode the array of bytes to message
//			got, err := NewFromBytes(bts)
//			fmt.Println(got,err)
//			if (err != nil) != tt.wantErr {
//				t.Errorf("NewFromBytes() error = %v, wantErr %v", err, tt.wantErr)
//				return
//			}
//			if !reflect.DeepEqual(got, tt.inAndOut) {
//				t.Errorf("NewFromBytes() = %v, want %v", got, tt.inAndOut)
//			}
//		})
//	}
//
//	// Test to see if error is raised when garbage is passed
//	got, err := NewFromBytes([]byte("Garbage String"))
//	if (err != nil) != true {
//		t.Errorf("NewFromBytes() error = %v, wantErr %v", err, true)
//		return
//	}
//	if got != nil {
//		t.Errorf("NewFromBytes() = %v, want %v", got, nil)
//	}
//}

//
//Tests:
//	- Message_SetKey
//func TestMessage_SetKey(t *testing.T) {
//	tests := []struct {
//		name  string
//		m     *Msg
//		input string
//		want  *Msg
//	}{
//		// TODO: add test cases.
//		{"Empty Msg", &Msg{}, "", &Msg{}},
//		{"add Key", &Msg{}, "One", &Msg{Key: "One"}},
//		{"Change Key", &Msg{Key: "Two"}, "One", &Msg{Key: "One"}},
//	}
//	for _, tt := range tests {
//		t.Run(tt.name, func(t *testing.T) {
//			tt.m.SetKey(tt.input)
//
//			if !reflect.DeepEqual(tt.m, tt.want) {
//				t.Errorf("After setting key m = %v, want %v", tt.m, tt.want)
//			}
//		})
//	}
//}
//
//Tests:
//	- Message_SetField
//func TestMessage_SetField(t *testing.T) {
//	type args struct {
//		key   string
//		value *MsgFieldValue
//	}
//	tests := []struct {
//		name string
//		prev MsgContent
//		args args
//		want MsgContent
//	}{
//		// TODO: add test cases.
//		{"Empty Msg", MsgContent{}, args{key: "", value: nil}, MsgContent{"": nil}},
//		{"add Data", MsgContent{}, args{key: "Greet", value: NewFieldValue("Hello", STRING)},
//			MsgContent{"Greet": NewFieldValue("Hello", STRING)}},
//		{"Change Data", MsgContent{"Number": NewFieldValue(3, INT)},
//			args{key: "Number", value: NewFieldValue(3.4, FLOAT)},
//			MsgContent{"Number": NewFieldValue(3.4, FLOAT)}},
//	}
//	for _, tt := range tests {
//		t.Run(tt.name, func(t *testing.T) {
//			msg := &Msg{mcontent: &tt.prev}
//			want := &Msg{mcontent: &tt.want}
//			if got := msg.SetField(tt.args.key, tt.args.value); !reflect.DeepEqual(got, want) {
//				t.Errorf("Msg.SetField() = %v, want %v", got, want)
//			}
//		})
//	}
//}
//
//// Tests:
////	- Message_ValueMap
//func TestMessage_ValueMap(t *testing.T) {
//	tests := []struct {
//		name     string
//		inAndOut MsgContent
//	}{
//		// TODO: add test cases.
//		{"Empty Map", MsgContent{}},
//		{"Map of length 1", MsgContent{"Number": NewFieldValue(7, INT)}},
//		{"Nil map", nil},
//	}
//	for _, tt := range tests {
//		t.Run(tt.name, func(t *testing.T) {
//			msg := Msg{mcontent: &tt.inAndOut}
//			if got := msg.Content(); !reflect.DeepEqual(got, tt.inAndOut) {
//				t.Errorf("Msg.MsgContent() = %v, want %v", got, tt.inAndOut)
//			}
//		})
//	}
//}
//
//// Tests:
////	- Message_Values
//func TestMessage_Values(t *testing.T) {
//	tests := []struct {
//		name string
//		in   MsgContent
//		want map[string]interface{}
//	}{
//		// TODO: add test cases.
//		{"Empty Map", MsgContent{}, map[string]interface{}{}},
//		{"Map of length 1", MsgContent{"Number": NewFieldValue(7, INT)}, map[string]interface{}{"Number": 7}},
//		{"Nil map", nil, nil},
//	}
//	for _, tt := range tests {
//		t.Run(tt.name, func(t *testing.T) {
//			msg := Msg{mcontent: &tt.in}
//			if got := msg.Values(); !reflect.DeepEqual(got, tt.want) {
//				t.Errorf("Msg.Values() = %v, want %v", got, tt.want)
//			}
//		})
//	}
//}
//
//// Tests:
////	- Message_Types
//func TestMessage_Types(t *testing.T) {
//	tests := []struct {
//		name string
//		in   MsgContent
//		want map[string]FieldValueType
//	}{
//		// TODO: add test cases.
//		{"Empty Map", MsgContent{}, map[string]FieldValueType{}},
//		{"Map of length 1", MsgContent{"Number": NewFieldValue(7, INT)}, map[string]FieldValueType{"Number": INT}},
//		{"Nil map", nil, nil},
//	}
//	for _, tt := range tests {
//		t.Run(tt.name, func(t *testing.T) {
//			msg := Msg{mcontent: &tt.in}
//			if got := msg.Types(); !reflect.DeepEqual(got, tt.want) {
//				t.Errorf("Msg.Values() = %v, want %v", got, tt.want)
//			}
//		})
//	}
//}
//
//// Tests:
////	- Message_FieldValue
//func TestMessage_FieldValue(t *testing.T) {
//	tests := []struct {
//		name  string
//		m     *Msg
//		field string
//		want  *MsgFieldValue
//	}{
//		// TODO: add test cases.
//		{"Empty Msg", &Msg{}, "Number", nil},
//		{"No val", &Msg{mcontent: &MsgContent{"Greet": NewFieldValue("Hello", STRING)}}, "Number", nil},
//		{"Yes val", &Msg{mcontent: &MsgContent{"Number": NewFieldValue("Hello", STRING)}},
//			"Number", NewFieldValue("Hello", STRING)},
//		{"Yes val", &Msg{mcontent: &MsgContent{"Greet": NewFieldValue("Nihao", STRING)}},
//			"Greet", NewFieldValue("Nihao", STRING)},
//	}
//	for _, tt := range tests {
//		t.Run(tt.name, func(t *testing.T) {
//			//if got := tt.m.FieldValue(tt.field); !reflect.DeepEqual(got, tt.want) {
//			//	t.Errorf("Msg.FieldValue() = %v, want %v", got, tt.want)
//			//}
//		})
//	}
//}
//
//// Tests:
////	- Message_PassThrough
//func TestMessage_Passthrough(t *testing.T) {
//	tests := []struct {
//		name string
//		m    *Msg
//		want bool
//	}{
//		// TODO: add test cases.
//		{"Empty Msg - No Pass", &Msg{}, false},
//		{"Done Msg - Pass", &Msg{mtype: DONE}, true},
//		{"Checkpoint Msg - Pass", &Msg{checkpointFlag: true}, true},
//		{"Other Msg - No Pass", &Msg{id: 2}, false},
//	}
//	for _, tt := range tests {
//		t.Run(tt.name, func(t *testing.T) {
//			if got := tt.m.ShouldPass(); got != tt.want {
//				t.Errorf("Msg.ShouldPass() = %v, want %v", got, tt.want)
//			}
//		})
//	}
//}
//
//// Tests:
////	- newMessageFactory
////	- MessageFactory_New
////	- newTraceMessageFactory
////	- MessageFactory_NewDoneMessage
//func TestMessageFactory(t *testing.T) {
//	type args struct {
//		networkId   uint
//		hubId       uint32
//		transformId uint32
//		value       MsgContent
//	}
//	tests := []struct {
//		name string
//		args args
//		want *Msg
//	}{
//		// TODO: add test cases.
//		{"Empty Msg", args{1, 0, 19, MsgContent{}},
//			&Msg{mcontent: MsgContent{},
//				pipelineId: 1, stageId: 0, processorId: 19, id: 1, traceFlag: false}},
//		{"Greet Msg", args{0, 0, 0, MsgContent{"Greet": NewFieldValue("Ohayo", STRING)}},
//			&Msg{mcontent: MsgContent{"Greet": NewFieldValue("Ohayo", STRING)},
//				pipelineId: 0, stageId: 0, processorId: 0, id: 1, traceFlag: false}},
//	}
//	for _, tt := range tests {
//		t.Run(tt.name, func(t *testing.T) {
//			mf := newMessageFactory(tt.args.networkId, tt.args.hubId, tt.args.transformId)
//			if got := mf.NewExecute(tt.args.value); !reflect.DeepEqual(got, tt.want) {
//				t.Errorf("MessageFactory.NewExecute() = %v, want %v", got, tt.want)
//			}
//
//			tt.want.traceFlag = true
//			tmf := newTraceMessageFactory(tt.args.networkId, tt.args.hubId, tt.args.transformId)
//			if got := tmf.NewExecute(tt.args.value); !reflect.DeepEqual(got, tt.want) {
//				t.Errorf("trace MessageFactory.NewExecute() = %v, want %v", got, tt.want)
//			}
//
//			doneMsg := mf.NewDone()
//			if got := doneMsg.IsDone(); got != true {
//				t.Errorf("Done Msg should have doneFlag = %v, got = %v", true, got)
//			}
//
//			doneMsg = mf.NewDone()
//			if got := doneMsg.IsDone(); got != true {
//				t.Errorf("Done Msg should have doneFlag = %v, got = %v", true, got)
//			}
//		})
//	}
//}
