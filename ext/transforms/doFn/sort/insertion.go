package sort

import (
	"container/list"

	"github.com/raralabs/canal/core/message"
	"github.com/raralabs/canal/utils/cast"
	"github.com/raralabs/canal/utils/extract"
)

type Insertion struct {
	cols      []string
	first     bool
	ordered   *list.List
	fieldType message.FieldValueType
	field     string
}

func NewInsertion(field string) *Insertion {
	return &Insertion{
		first:   true,
		ordered: list.New(),
		field:   field,
	}
}

func (ins *Insertion) Add(mContent *message.OrderedContent) {
	if ins.first {
		ins.first = false
		ins.cols = extract.Columns(mContent)
		if v, ok := mContent.Get(ins.field); ok {
			ins.fieldType = v.ValueType()
		}
	}

	items := make([]*message.MsgFieldValue, len(ins.cols))
	keyIndex := 0
	for i, key := range ins.cols {
		if v, ok := mContent.Get(key); !ok {
			return
		} else {
			items[i] = v
		}

		if ins.field == key {
			keyIndex = i
		}
	}

	// Perform insertion
	if ins.ordered.Len() == 0 {
		ins.ordered.PushBack(items)
		return
	}

	for e := ins.ordered.Front(); e != nil; e = e.Next() {
		currContent, _ := e.Value.([]*message.MsgFieldValue)
		switch ins.fieldType {
		case message.INT:
			v1 := items[keyIndex].Value()
			v2 := currContent[keyIndex].Value()
			newVal, ok1 := cast.TryInt(v1)
			currVal, ok2 := cast.TryInt(v2)

			// Handle uint64
			if !ok1 || !ok2 {
				x, _ := v1.(uint64)
				y, _ := v2.(uint64)

				if x >= y {
					ins.ordered.InsertBefore(items, e)
					return
				}
				break
			}

			if newVal >= currVal {
				ins.ordered.InsertBefore(items, e)
				return
			}

		case message.FLOAT:
			newVal, _ := cast.TryFloat(items[keyIndex].Value())
			currVal, _ := cast.TryFloat(currContent[keyIndex].Value())

			if newVal >= currVal {
				ins.ordered.InsertBefore(items, e)
				return
			}

		case message.STRING:
			newVal, _ := cast.TryString(items[keyIndex].Value())
			currVal, _ := cast.TryString(currContent[keyIndex].Value())

			if newVal >= currVal {
				ins.ordered.InsertBefore(items, e)
				return
			}
		}
	}

	ins.ordered.PushBack(items)
}

func (ins *Insertion) Iterate() chan []*message.MsgFieldValue {
	ch := make(chan []*message.MsgFieldValue)

	go func() {
		defer close(ch)

		for e := ins.ordered.Front(); e != nil; e = e.Next() {
			ch <- e.Value.([]*message.MsgFieldValue)
		}
	}()

	return ch
}

func (ins *Insertion) Columns() []string {
	return ins.cols
}
