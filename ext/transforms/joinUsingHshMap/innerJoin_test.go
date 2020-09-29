package joinUsingHshMap

//import (
//	"github.com/raralabs/canal/core/message/content"
//	"github.com/stretchr/testify/assert"
//	"reflect"
//	"testing"
//)
//
//func TestNewInnerJoin(t *testing.T) {
//	t.Run("create new innerjoin", func(t *testing.T) {
//		innerJoin:= NewInnerJoin(HASH,"path1","path2",[]string{"id","roll no"})
//		assert.Equal(t,innerJoin.JoinStrategy,HASH,"strategy mismatch")
//		assert.Equal(t,reflect.TypeOf(innerJoin.hashTable).String(),"*joinUsingHshMap.HashTable","table type didn't match")
//	})
//}

//func TestInnerJoin_Join(t *testing.T) {
//	type testCase struct{
//		testName int
//		stream1  content.IContent
//		stream2  content.IContent
//		selectFields []string
//
//	}
//	msgContent:= content.New()
//	tests := []testCase{
//		{1,msgContent.Add("name",content.NewFieldValue("test",content.STRING)),
//			msgContent.Add("age",content.NewFieldValue(14,content.INT)),
//		[]string{"age"},
//		},
//		{2,msgContent.Add("name",content.NewFieldValue("test",content.STRING)),
//			msgContent.Add("age",content.NewFieldValue(14,content.INT)),
//			[]string{"name"},
//		},
//		{3,msgContent.Add("name",content.NewFieldValue("test",content.STRING)),
//			msgContent.Add("age",content.NewFieldValue(14,content.INT)),
//			[]string{"name","age"},
//		},
//		{4,msgContent.Add("name",content.NewFieldValue("test",content.STRING)),
//			msgContent.Add("age",content.NewFieldValue(14,content.INT)),
//			[]string{"first_name"},
//		},
//	}
//	innerJoin:= NewInnerJoin(HASH)
//	for _,test := range(tests){
//		wantContent := content.New()
//		if test.testName != 4{
//			msgContent := innerJoin.Join(test.stream1,test.stream2,test.selectFields)
//			if test.testName == 1{
//				assert.Equal(t,wantContent.Add("age",content.NewFieldValue(14,content.INT)),msgContent)
//			}else if test.testName ==2{
//				assert.Equal(t,wantContent.Add("name",content.NewFieldValue("test",content.STRING)),msgContent)
//			}else if test.testName ==3{
//				wantContent.Add("age",content.NewFieldValue(14,content.INT))
//				assert.Equal(t,wantContent.Add("name",content.NewFieldValue("test",content.STRING)),msgContent)
//			}
//		}else{
//			assert.Panics(t, func() {
//				innerJoin.Join(test.stream1,test.stream2,test.selectFields)
//			},"code didn't panic")
//		}
//
//	}


	//}
