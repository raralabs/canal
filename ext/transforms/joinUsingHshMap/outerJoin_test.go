package joinUsingHshMap

import (
	"github.com/raralabs/canal/core/message/content"
	"github.com/raralabs/canal/core/pipeline"
	"github.com/stretchr/testify/assert"
	"reflect"
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
		selectFields []string

	}
	msgContent:= content.New()

	tests := []testCase{
		{1,msgContent.Add("name",content.NewFieldValue("test",content.STRING)),
			[]string{"age"},
		},
		{2,msgContent.Add("name",content.NewFieldValue("test",content.STRING)),
			[]string{"name"},
		},
		{3,msgContent.Add("name",content.NewFieldValue("test",content.STRING)),
			[]string{"name","age"},
		},
		{4,msgContent.Add("name",content.NewFieldValue("test",content.STRING)),
			[]string{"first_name"},
		},
	}
	for _,test := range(tests){
		outerJoin:= NewOuterJoin(HASH,"path1","path2",test.selectFields,LEFTOUTER)
		t.Run("process stream 1st", func(t *testing.T) {
			outerJoin.ProcessStreamFirst(test.stream1,test.selectFields)

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