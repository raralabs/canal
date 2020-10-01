package joinUsingHshMap

import (
	"github.com/dorin131/go-data-structures/linkedlist"
	"github.com/raralabs/canal/core/message/content"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestNewHashMap(t *testing.T) {
	t.Run("test new hash map", func(t *testing.T) {
		hashTable := NewHashMap()
		assert.Equal(t,hashTable,&HashTable{data: [arrayLength]*linkedlist.LinkedList{}},"has table creation failed")
	})
}
//

func TestHashTable_concatKeys(t *testing.T){
	test := []interface{}{1,"Hari","Bahadur"}
	t.Run("concat keys", func(t *testing.T) {
		concatenatedValue := concatKeys(test)
		assert.Equal(t,"1 Hari Bahadur",concatenatedValue,"concatKeys function doesn't perform as expected")
	})
}

func TestHashTable_createHash(t *testing.T){

	type testType struct {
		wantCollision bool
		msg1 	string
		msg2 	string
	}
	tests:= []testType{
		{false,"1 test case 1","1 non colliding item"},
		{true,"same message","same message"},
		{false,"reverse msg","gsm esrever"},
	}
	for _,test := range tests{
		t.Run("create Hash", func(t *testing.T) {
			hash1 := createHash(test.msg1)
			hash2 := createHash(test.msg2)
			if test.wantCollision==false{
				assert.NotEqual(t,hash1,hash2,"collision detected for different values")
			}else{
				assert.Equal(t,hash1,hash2)
			}
		})
	}
}


//Tests
//- Set
//- Get
func TestHashTable_Set_Get(t *testing.T) {

	type testType struct{
		msg  content.MsgFieldValue
		concatKey string
	}
	testCases:= []testType{
		{content.MsgFieldValue{"dummy",content.STRING},"1 year"},
		{content.MsgFieldValue{"dummy dummy",content.STRING},"2 year"},
		{content.MsgFieldValue{"dummy dummy dummy ",content.STRING},"3 year"},

	}
	hashTable := NewHashMap()
	for _,test := range(testCases){
		t.Run("set values in the hash table", func(t *testing.T) {
			_,ok := hashTable.Get(test.concatKey)
			assert.Equal(t,false,ok,"hash table should be empty at the begining")
			hashTable.Set(test.msg,test.concatKey)
			value,ok := hashTable.Get(test.concatKey)
			hashTable.iterate()
			assert.Equal(t,true,ok)
			assert.Equal(t,value,test.msg)
			hashTable.iterate()
			value,ok = hashTable.Get(test.concatKey)
			assert.Equal(t,false,ok)
			assert.Equal(t,nil,value)

		})
	}
}