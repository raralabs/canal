package content

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
func NewFieldValue(value interface{}, valueType FieldValueType) MsgFieldValue {
	m := MsgFieldValue{
		Val:     value,
		ValType: valueType,
	}

	return m
}

func (mfv MsgFieldValue) Value() interface{} {
	return mfv.Val
}

func (mfv MsgFieldValue) ValueType() FieldValueType {
	return mfv.ValType
}

func (mfv MsgFieldValue) String() string {
	return fmt.Sprintf("%s(%v)", mfv.ValType, mfv.Val)
}
