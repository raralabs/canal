package joinUsingHshMap

import (
	"github.com/raralabs/canal/core/message/content"
	"github.com/raralabs/canal/core/pipeline"
	"github.com/stretchr/testify/assert"
	"reflect"
	"sort"
	"testing"
)

//Tests
// - NewOuterJoin
// - Type()
func TestNewOuterJoin(t *testing.T) {
		t.Run("create new outerjoin", func(t *testing.T) {
			outerJoin:= NewOuterJoin(HASH,"path1","path2",[]string{"id","roll no"},LEFTOUTER)
			assert.Equal(t,outerJoin.JoinStrategy,HASH,"strategy mismatch")
			assert.Equal(t,reflect.TypeOf(outerJoin.hashTable).String(),"*joinUsingHshMap.HashTable","table type didn't match")
			assert.Equal(t,outerJoin.mergeLock,false,"lock already enabled")
			assert.Equal(t,LEFTOUTER,outerJoin.subType)
			assert.Equal(t,pipeline.MsgRouteParam("path1"),outerJoin.firstPath,"path mismatch")
			assert.Equal(t,pipeline.MsgRouteParam("path2"),outerJoin.secondPath,"path mismatch")
		})
}

func TestOuterJoin_ProcessStreamFirst(t *testing.T) {
	type testCase struct{
		testName int
		stream1  content.IContent
		stream2  content.IContent
		selectFields []string

	}
	msgContent:= content.New()

	tests := []testCase{
		{1,msgContent.Add("data",content.NewFieldValue("test",content.STRING)),
			msgContent.Add("data",content.NewFieldValue(14,content.INT)),
			[]string{"age"},
		},
		{2,msgContent.Add("data",content.NewFieldValue("test",content.STRING)),
			msgContent.Add("data",content.NewFieldValue("test2",content.STRING)),
			[]string{"name"},
		},
		{3,msgContent.Add("name",content.NewFieldValue("test",content.STRING)),
			msgContent.Add("data",content.NewFieldValue(14,content.INT)),
			[]string{"name","age"},
		},
		{4,msgContent.Add("data",content.NewFieldValue("test",content.STRING)),
			msgContent.Add("data",content.NewFieldValue("test2",content.INT)),
			[]string{"name"},
		},
	}
	for _,test := range(tests){
		outerJoin:= NewOuterJoin(HASH,"path1","path2",test.selectFields,LEFTOUTER)
		t.Run("process stream 1st", func(t *testing.T) {
			outerJoin.ProcessStreamFirst(test.stream1,[]string{"data"})
			outerJoin.ProcessStreamSec(test.stream2,[]string{"data"})
			linkedList := outerJoin.hashTable.data
			for _,table := range(linkedList){
				if table!=nil {
					node :=table.Head
					for node.Next!= nil{
						assert.Equal(t,test.stream1,node.Data.(listData).value.(content.IContent),"integrity of msg not preserved")
						node = node.Next

					}
				}
			}

		})
	}
}

func TestOuterJoin_ProcessStreamSec(t *testing.T) {
	type testStream struct{
		fields 		[]string
		stream 		content.IContent
	}
	type testCase struct{
		outJoin    StreamJoin
		testName string
		streamMsg  testStream
		selectFields []string

	}
	msgContent:= content.New()
	join1 := NewOuterJoin(HASH,"path1","path2",[]string{"name"},LEFTOUTER)
	join2 := NewOuterJoin(HASH,"path1","path2",[]string{"age"},LEFTOUTER)
	tests := []testCase{
		{	join1,
			"test1",
			testStream{[]string{"name"},
				msgContent.Add("name",content.NewFieldValue("kumar",content.STRING))},
				[]string{"name"}},
		{	join1,
			"test2",
			testStream{[]string{"name"},
				msgContent.Add("kishore",content.NewFieldValue("kishore",content.STRING))},
			[]string{"name"}},
		{	join2,
			"test3",
			testStream{[]string{"age"},
				msgContent.Add("age",content.NewFieldValue("14",content.STRING))},
			[]string{"age"}},
		{	join2,
			"test4",
			testStream{[]string{"age"},
				msgContent.Add("age",content.NewFieldValue("14",content.STRING))},
			[]string{"age"}},
	}
	counter := 0
	for _,test := range tests{
		t.Run("process stream 2nd", func(t *testing.T) {
			if counter%2 == 0 {
				test.outJoin.ProcessStreamFirst(test.streamMsg.stream,test.selectFields)
			}else{
				result,ok := test.outJoin.ProcessStreamSec(test.streamMsg.stream,test.selectFields)
				assert.Equal(t,true,ok)
				assert.Equal(t,result,test.streamMsg.stream,test.selectFields)
			}
			counter++
		})
	}
}

func TestOuterJoin_mergeContent(t *testing.T){
	join := NewOuterJoin(HASH,"path1","path2",[]string{"name"},LEFTOUTER)
	join.selectFields = []string{"*"}
	type testCase struct{
		testName string
		streamMsg1  content.IContent
		streamMsg2 	content.IContent
		selectFields []string
	}
	msgContent1 := content.New()
	msgContent2 := content.New()
	tests :=[]testCase{
		{"test1",msgContent1.Add("name",content.NewFieldValue("kumar",content.STRING)),
			msgContent2.Add("full_name",content.NewFieldValue("kumar Rai",content.STRING)),[]string{"name","full_name"}},

		{"test2",msgContent1.Add("address",content.NewFieldValue("lalitpur",content.STRING)),
			msgContent2.Add("age",content.NewFieldValue(13,content.INT)),[]string{"age","address"}},

		{"test3",msgContent1.Add("last_name",content.NewFieldValue("Rai",content.STRING)),
			msgContent2.Add("id",content.NewFieldValue(1,content.INT)),[]string{"*"}},
	}
	for _,test := range tests{
		join.selectFields = test.selectFields
		merged :=  join.mergeContent(test.streamMsg1,test.streamMsg2)
		if test.selectFields[0]=="*"{
			a := []string{"address", "last_name", "full_name", "age", "id", "name"}
			sort.Strings(a)
			b:= merged.Keys()
			sort.Strings(b)
			assert.Equal(t,a,b,"Extracted keys are different")
		}else{
			assert.Equal(t,merged.Keys(),test.selectFields)
		}
	}
}