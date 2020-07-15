package stream_math

import "sync"

type FreqCounter struct {
	valMu  *sync.Mutex
	values map[interface{}]uint64
}

func NewFreqCounter() *FreqCounter {
	return &FreqCounter{
		valMu:  &sync.Mutex{},
		values: make(map[interface{}]uint64),
	}
}

func (fc *FreqCounter) Add(v interface{}) {
	fc.valMu.Lock()
	defer fc.valMu.Unlock()

	if _, ok := fc.values[v]; ok {
		fc.values[v]++
	} else {
		fc.values[v] = uint64(1)
	}
}

func (fc *FreqCounter) Remove(v interface{}) {
	fc.valMu.Lock()
	defer fc.valMu.Unlock()

	if cnt, ok := fc.values[v]; ok {
		if cnt > 0 {
			fc.values[v]--
		}
	}
}

func (fc *FreqCounter) Values() map[interface{}]uint64 {
	fc.valMu.Lock()
	defer fc.valMu.Unlock()

	return fc.values
}

func (fc *FreqCounter) Reset() {
	for k := range fc.values {
		delete(fc.values, k)
	}
}
