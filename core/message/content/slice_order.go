package content

import (
	"fmt"
)

type SliceOrdered struct {
	keys    []string
	content map[string]MsgFieldValue
}

func NewSliceOrdered() IContent {
	c := make(map[string]MsgFieldValue)

	return &SliceOrdered{
		keys:    make([]string, 8),
		content: c,
	}
}
func (oc *SliceOrdered) Remove(key string){
	delete(oc.content,key)
}

func (oc *SliceOrdered) Copy() IContent {
	cpy := New()
	for _, k := range oc.Keys() {
		v, _ := oc.Get(k)
		cpy.Add(k, v)
	}
	return cpy
}

func (oc *SliceOrdered) Get(key string) (MsgFieldValue, bool) {
	val, ok := oc.content[key]
	return val, ok
}

func (oc *SliceOrdered) Add(key string, value MsgFieldValue) {
	if _, ok := oc.content[key]; !ok {
		oc.keys = append(oc.keys, key)
	}
	oc.content[key] = value
}

func (oc *SliceOrdered) Len() int {
	return len(oc.content)
}

func (oc *SliceOrdered) Keys() []string {
	return oc.keys
}

// Values returns a map with just keys and values in the message, without type
// information in order.
func (oc *SliceOrdered) Values() map[string]interface{} {
	if oc.content == nil {
		return nil
	}

	values := make(map[string]interface{})

	for _, k := range oc.Keys() {
		v := oc.content[k]
		values[k] = v.Value()
	}

	return values
}

// Types returns a map with just keys and values types in the message, without
// actual in order.
func (oc *SliceOrdered) Types() map[string]FieldValueType {

	if oc.content == nil {
		return nil
	}

	types := make(map[string]FieldValueType)

	for _, k := range oc.Keys() {
		v := oc.content[k]
		types[k] = v.ValueType()
	}

	return types
}

// String returns string representation in order
func (oc *SliceOrdered) String() string {
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
