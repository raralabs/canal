package main

import (
	"container/list"
	"fmt"
)

type insertion struct {
	values *list.List
}

func (in *insertion) add(v int) {
	if in.values.Len() == 0 {
		in.values.PushBack(v)
		return
	}

	for e := in.values.Front(); e != nil; e = e.Next() {
		oldVal := e.Value.(int)

		if v > oldVal {
			in.values.InsertBefore(v, e)
			return
		}
	}
	in.values.PushBack(v)
}

func (in *insertion) iterate() chan int {
	ch := make(chan int)

	go func() {
		defer close(ch)

		for e := in.values.Front(); e != nil; e = e.Next() {
			ch <- e.Value.(int)
		}
	}()

	return ch
}

func main() {
	vals := []int{1, -1, 0, 12, 59, -100, 11}

	sorter := insertion{values: list.New()}
	for _, v := range vals {
		sorter.add(v)
	}

	for out := range sorter.iterate() {
		fmt.Print(out, " ")
	}
	fmt.Println()
}
