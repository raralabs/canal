package message

import (
	"container/list"
	"fmt"
)

type OrderedContent struct {
	keyList *list.List
	content map[string]*MsgFieldValue
}

func NewOrderedContent() *OrderedContent {
	kl := list.New()
	c := make(map[string]*MsgFieldValue)

	return &OrderedContent{
		keyList: kl,
		content: c,
	}
}

func (oc *OrderedContent) Get(key string) (*MsgFieldValue, bool) {
	val, ok := oc.content[key]
	return val, ok
}

func (oc *OrderedContent) Add(key string, value *MsgFieldValue) {
	// Add the key to list if it is new
	if _, ok := oc.content[key]; !ok {
		oc.keyList.PushBack(key)
	}
	oc.content[key] = value
}

func (oc *OrderedContent) Front() *list.Element {
	return oc.keyList.Front()
}

func (oc *OrderedContent) Back() *list.Element {
	return oc.keyList.Back()
}

// Values returns a map with just keys and values in the message, without type
// information in order.
func (oc *OrderedContent) Values() map[string]interface{} {
	if oc.content == nil {
		return nil
	}

	values := make(map[string]interface{})
	for e := oc.Front(); e != nil; e = e.Next() {
		// Since we are only inserting strings, so no check required
		k, _ := e.Value.(string)
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
func (oc *OrderedContent) Types() map[string]FieldValueType {

	if oc.content == nil {
		return nil
	}

	types := make(map[string]FieldValueType)
	for e := oc.Front(); e != nil; e = e.Next() {
		// Since we are only inserting strings, so no check required
		k, _ := e.Value.(string)
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
func (oc *OrderedContent) String() string {
	var values string
	for e := oc.Front(); e != nil; e = e.Next() {
		// Since we are only inserting strings, so no check required
		k, _ := e.Value.(string)
		v := oc.content[k]

		if values != "" {
			values += " "
		}
		values += fmt.Sprintf("%s:%s", k, v.String())
	}
	return fmt.Sprintf("MsgContent{%s}", values)
}
