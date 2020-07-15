package sort_algo

import "container/list"

type Insertion struct {
	less func(old, new interface{}) bool
	ordered   *list.List
}

func NewInsertion(less func(old, new interface{}) bool) *Insertion {
	return &Insertion{
		less: less,
		ordered: list.New(),
	}
}

func (ins *Insertion) Add(v interface{}) {
	// Perform insertion
	if ins.ordered.Len() == 0 {
		ins.ordered.PushBack(v)
		return
	}

	for e := ins.First(); e != nil; e = e.Next() {
		old := e.Value
		if ins.less(old, v) {
			ins.ordered.InsertBefore(v, e)
			return
		}
	}
	ins.ordered.PushBack(v)
}

func (ins *Insertion) First() *list.Element {
	return ins.ordered.Front()
}

func (ins *Insertion) Last() *list.Element {
	return ins.ordered.Back()
}
