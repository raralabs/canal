package dstr

import (
	"errors"
	"sync/atomic"
)

var disposedError = errors.New(`Queue has been disposed.`)

type node struct {
	data interface{}
}

// RoundRobin is a data structure in which elements can be inserted, but can't be deleted.
// On insertion on buffer full, the oldest elements are removed.
type RoundRobin struct {
	nodes                        []*node
	disposed, head, size, filled uint64 // The size of the buffer
}

func (rr *RoundRobin) init(size uint64) {
	rr.size = size
	rr.nodes = make([]*node, size)
	for i := uint64(0); i < size; i++ {
		rr.nodes[i] = &node{}
	}
}

// Put inserts an item to the roundrobin. If roundrobin is full, the oldest data is removed
func (rr *RoundRobin) Put(item interface{}) error {
	var n *node
	pos := atomic.LoadUint64(&rr.head)

	if atomic.LoadUint64(&rr.disposed) == 1 {
		return disposedError
	}

	n = rr.nodes[pos]
	n.data = item

	if atomic.LoadUint64(&rr.filled) < atomic.LoadUint64(&rr.size) {
		atomic.AddUint64(&rr.filled, 1)
	}

	atomic.CompareAndSwapUint64(&rr.head, pos, pos+1)
	rr.head %= atomic.LoadUint64(&rr.size)

	return nil
}

// GetAll returns all the stored data in the roundrobin in order
func (rr *RoundRobin) GetAll() ([]interface{}, error) {

	if atomic.LoadUint64(&rr.disposed) == 1 {
		return nil, disposedError
	}

	filled := atomic.LoadUint64(&rr.filled)
	head := atomic.LoadUint64(&rr.head)
	out := make([]interface{}, filled)

	if filled < atomic.LoadUint64(&rr.size) {
		for i := uint64(0); i < head; i++ {
			out[i] = rr.nodes[i].data
		}
	} else {
		for i := head; i < rr.size; i++ {
			out[i-head] = rr.nodes[i].data
		}
		for i := uint64(0); i < head; i++ {
			out[i+head+1] = rr.nodes[i].data
		}
	}

	return out, nil
}

// Get returns the data at the ith position
func (rr *RoundRobin) Get(i uint64) (interface{}, error) {

	if atomic.LoadUint64(&rr.disposed) == 1 {
		return nil, disposedError
	}

	filled := atomic.LoadUint64(&rr.filled)
	head := atomic.LoadUint64(&rr.head)
	var out interface{}

	if filled < atomic.LoadUint64(&rr.size) {
		out = rr.nodes[i].data
	} else {
		out = rr.nodes[(i+head)%atomic.LoadUint64(&rr.size)].data
	}

	return out, nil
}

// Len returns the number of items in the queue.
func (rr *RoundRobin) Len() uint64 {
	return atomic.LoadUint64(&rr.filled)
}

// Cap returns the capacity of this ring buffer.
func (rr *RoundRobin) Cap() uint64 {
	return atomic.LoadUint64(&rr.size)
}

// Dispose will dispose of the roundrobin
func (rr *RoundRobin) Dispose() {
	atomic.CompareAndSwapUint64(&rr.disposed, 0, 1)
}

// NewRoundRobin will allocate, initialize, and return a roundrobin
// with the specified size.
func NewRoundRobin(size uint64) *RoundRobin {
	rr := &RoundRobin{}
	rr.init(size)
	return rr
}
