package message

import "fmt"

// FieldValueType represents the supported types for the value of message
type FieldValueType uint8

// These are the currently supported types
const (
	INT    FieldValueType = iota + 1 // Represents integer number
	FLOAT                            // Represents floating point number
	STRING                           // Represents string
	BOOL                             // Represents boolean value

	NONE
)

func (fvt FieldValueType) String() string {
	switch fvt {
	case INT:
		return "int"
	case FLOAT:
		return "float"
	case STRING:
		return "str"
	case BOOL:
		return "bool"
	case NONE:
		return "none"
	}

	return "unknown"
}

// A MsgFieldValue implements the MessageAttr interface.
type MsgFieldValue struct {
	Val     interface{}
	ValType FieldValueType
}

// Creates a new MsgFieldValue and returns it.
func NewFieldValue(value interface{}, valueType FieldValueType) *MsgFieldValue {
	m := &MsgFieldValue{}
	m.SetValue(value)
	m.SetType(valueType)

	return m
}

//func (mfv MsgFieldValue) Value() interface{} {
//	return mfv.Val
//}
//
//func (mfv MsgFieldValue) ValueType() FieldValueType {
//	return mfv.ValType
//}

func (mfv *MsgFieldValue) Value() interface{} {
	return mfv.Val
}

func (mfv *MsgFieldValue) ValueType() FieldValueType {
	return mfv.ValType
}

func (mfv *MsgFieldValue) SetValue(value interface{}) {
	mfv.Val = value
}

func (mfv *MsgFieldValue) SetType(valueType FieldValueType) {
	mfv.ValType = valueType
}

func (mfv *MsgFieldValue) String() string {
	return fmt.Sprintf("%s(%v)", mfv.ValType, mfv.Val)
}

// A MsgContent is the actual data stored by the message.
type MsgContent map[string]*MsgFieldValue

// AddMessageValue adds a value with given key and type to the MessageContent.
func (content MsgContent) AddMessageValue(key string, value *MsgFieldValue) {
	content[key] = value
}

// Values returns a map with just keys and values in the message, without type
// information.
func (content MsgContent) Values() map[string]interface{} {
	if content == nil {
		return nil
	}

	values := make(map[string]interface{})
	for k, v := range content {
		if v == nil {
			values[k] = nil
		} else {
			values[k] = v.Value()
		}
	}

	return values
}

// Types returns a map with just keys and values types in the message, without
// actual.
func (content MsgContent) Types() map[string]FieldValueType {

	if content == nil {
		return nil
	}

	types := make(map[string]FieldValueType)
	for k, v := range content {
		if v == nil {
			types[k] = NONE
		} else {
			types[k] = v.ValueType()
		}
	}

	return types
}

func (content MsgContent) String() string {
	var values string
	for k, v := range content {
		if values != "" {
			values += " "
		}
		values += fmt.Sprintf("%s:%s", k, v.String())
	}
	return fmt.Sprintf("MsgContent{%s}", values)
}
