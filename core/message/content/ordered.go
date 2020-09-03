package content

import (
	"container/list"
	"fmt"
)

type Ordered struct {
	keyList *list.List
	content map[string]MsgFieldValue
}

func NewOrdered() IContent {
	kl := list.New()
	c := make(map[string]MsgFieldValue)

	return Ordered{
		keyList: kl,
		content: c,
	}
}

func (oc Ordered) Copy() IContent {
	cpy := NewOrdered()
	for k, v := range oc.content {
		cpy.Add(k, v)
	}
	return cpy
}


func (oc Ordered) Get(key string) (MsgFieldValue, bool) {
	val, ok := oc.content[key]
	return val, ok
}

func (oc *Ordered) Remove(key string){
	delete(oc.content,key)
}


func (oc Ordered) Add(key string, value MsgFieldValue) IContent {
	// Add the key to list if it is new
	if _, ok := oc.content[key]; !ok {
		oc.keyList.PushBack(key)
	}
	oc.content[key] = value

	return oc
}

func (oc Ordered) Len() int {
	return len(oc.content)
}

func (oc *Ordered) first() *list.Element {
	return oc.keyList.Front()
}

func (oc *Ordered) last() *list.Element {
	return oc.keyList.Back()
}


func (oc Ordered) Keys() []string {
	keys := make([]string, oc.keyList.Len())
	i := 0
	for e := oc.first(); e != nil; e = e.Next() {
		k, _ := e.Value.(string)
		keys[i] = k
		i++
	}

	return keys
}

// Values returns a map with just keys and values in the message, without type
// information in order.
func (oc Ordered) Values() map[string]interface{} {
	if oc.content == nil {
		return nil
	}

	values := make(map[string]interface{})
	for e := oc.first(); e != nil; e = e.Next() {
		// Since we are only inserting strings, so no check required
		k, _ := e.Value.(string)
		v := oc.content[k]
		values[k] = v.Value()
	}

	return values
}

// Types returns a map with just keys and values types in the message, without

// actual in order.
func (oc Ordered) Types() map[string]FieldValueType {
	if oc.content == nil {
		return nil
	}

	types := make(map[string]FieldValueType)
	for e := oc.first(); e != nil; e = e.Next() {
		// Since we are only inserting strings, so no check required
		k, _ := e.Value.(string)
		v := oc.content[k]
		types[k] = v.ValueType()
	}

	return types
}

// String returns string representation in order
func (oc Ordered) String() string {
	var values string
	for e := oc.first(); e != nil; e = e.Next() {
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
