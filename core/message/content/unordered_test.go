package content

import (
	"github.com/stretchr/testify/assert"
	"testing"
)


func TestUnordered_attr(t *testing.T) {
	//var msg *Msg

	msgValue1 := NewFieldValue("xyz", STRING)
	msgValue2 := NewFieldValue(12, INT)
	msgContent := New()
	msgContent.Add("name", msgValue1)
	msgContent.Add("roll", msgValue2)
	assert.Equal(t, map[string]FieldValueType{"name":STRING ,"roll":INT}, msgContent.Types(), "Types doesn't match expected")
	assert.Equal(t, []string([]string{"name", "roll"}), msgContent.Keys(), "Inserted Keys and Keys returned needs to be same.")
	assert.Equal(t, map[string]interface{}(map[string]interface{}{"name": "xyz", "roll": 12}), msgContent.Values(), "Inserted values and returned values needs to be same.")
	assert.Equal(t, 2, msgContent.Len(), "lengths should be equal to the number of content added.")
	assert.Equal(t, "MsgContent{name:str(xyz) roll:int(12)}", msgContent.String(), "should return the string format of the content")
	newCopy:= msgContent.Copy()
	assert.Equal(t, newCopy, msgContent, "copy must match original")
	keys:=[]string{"name","roll"}
	values:=[]interface{}{"xyz",12}
	for idx,key := range (keys){
		name,_ := msgContent.Get(key)
		_,isString :=name.Val.(string)
		if isString{
			data := name.Val.(string)
			assert.Equal(t, values[idx],data, "lengths should be equal to the number of content added.")
		}else{
			data := name.Val.(int)
			assert.Equal(t, values[idx],data, "lengths should be equal to the number of content added.")
		}

	}

}