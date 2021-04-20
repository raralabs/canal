package content

import (
	"github.com/stretchr/testify/assert"
	"reflect"
	"testing"
)

//Tests
// -Tests all the attributes of the Ordered Type
func TestOrdered_attr(t *testing.T) {
	//var msg *Msg

	msgValue1 := NewFieldValue("xyz", STRING)
	msgValue2 := NewFieldValue(12, INT)
	msgContent := NewOrdered()
	msgContent.Add("name", msgValue1)
	msgContent.Add("roll", msgValue2)

	newCopy := msgContent.Copy()

	tests := []struct {
		name     string
		expected interface{}
		actual   interface{}
		message  string
	}{
		{
			"Types Check",
			map[string]FieldValueType{"name": STRING, "roll": INT},
			msgContent.Types(),
			"Types doesn't match expected",
		},
		{
			"Keys Check",
			[]string([]string{"name", "roll"}),
			msgContent.Keys(),
			"Inserted Keys and Keys returned needs to be same.",
		},
		{
			"Values Check",
			map[string]interface{}{"name": "xyz", "roll": 12},
			msgContent.Values(),
			"Inserted values and returned values needs to be same",
		},
		{
			"Length Check",
			2,
			msgContent.Len(),
			"lengths should be equal to the number of content added",
		},
		{
			"Stringify Check",
			"MsgContent{name:str(xyz) roll:int(12)}",
			msgContent.String(),
			"should return the string format of the content",
		},
		{
			"Copy Check",
			newCopy,
			msgContent,
			"copy must match original",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if !reflect.DeepEqual(tt.expected, tt.actual) {
				t.Errorf("Want: %v\nGot: %v", tt.expected, tt.actual)
			}
		})
	}

	keys := []string{"name", "roll"}
	values := []interface{}{"xyz", 12}
	for idx, key := range keys {
		name, _ := msgContent.Get(key)
		_, isString := name.Val.(string)
		if isString {
			data := name.Val.(string)
			assert.Equal(t, values[idx], data, "lengths should be equal to the number of content added.")
		} else {
			data := name.Val.(int)
			assert.Equal(t, values[idx], data, "lengths should be equal to the number of content added.")
		}

	}
	//test to check if order is maintained

}
