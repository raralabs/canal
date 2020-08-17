package content

import (
	"fmt"
)

type Unordered struct {
	content map[string]*MsgFieldValue
}

func NewUnordered() IContent {
	c := make(map[string]*MsgFieldValue)

	return &Unordered{
		content: c,
	}
}

func (oc *Unordered) Copy() IContent {
	cpy := New()
	for _, k := range oc.Keys() {
		v, _ := oc.Get(k)
		cpy.Add(k, v)
	}
	return cpy
}

func (oc *Unordered) Get(key string) (*MsgFieldValue, bool) {
	val, ok := oc.content[key]
	return val, ok
}

func (oc *Unordered) Add(key string, value *MsgFieldValue) {
	oc.content[key] = value
}

func (oc *Unordered) Len() int {
	return len(oc.content)
}

func (oc *Unordered) Keys() []string {
	keys := make([]string, 0, len(oc.content))
	for k := range oc.content {
		keys = append(keys, k)
	}
	return keys
}

// Values returns a map with just keys and values in the message, without type
// information in order.
func (oc *Unordered) Values() map[string]interface{} {
	if oc.content == nil {
		return nil
	}

	values := make(map[string]interface{})

	for _, k := range oc.Keys() {
		v := oc.content[k]
		if v == nil {
			values[k] = nil
		} else {
			values[k] = v.Value()
		}
	}

	return values
}

// Types returns a map with just keys and values types in the message, without
// actual in order.
func (oc *Unordered) Types() map[string]FieldValueType {

	if oc.content == nil {
		return nil
	}

	types := make(map[string]FieldValueType)

	for _, k := range oc.Keys() {
		v := oc.content[k]
		if v == nil {
			types[k] = NONE
		} else {
			types[k] = v.ValueType()
		}
	}

	return types
}

// String returns string representation in order
func (oc *Unordered) String() string {
	var values string

	for _, k := range oc.Keys() {
		v := oc.content[k]

		if values != "" {
			values += " "
		}
		values += fmt.Sprintf("%s:%s", k, v.String())
	}
	return fmt.Sprintf("MsgContent{%s}", values)
}
