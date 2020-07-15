package stream_math

import "sync"

type FreqCounter struct {
	valMu   *sync.Mutex
	values  map[interface{}]uint64
	totalFq uint64
}

func NewFreqCounter() *FreqCounter {
	return &FreqCounter{
		valMu:  &sync.Mutex{},
		values: make(map[interface{}]uint64),
		totalFq: uint64(0),
	}
}

// Add increases the count for v and returns it if the
// count was 0 or non-existent before it.
func (fc *FreqCounter) Add(v interface{}) interface{} {
	fc.valMu.Lock()
	defer fc.valMu.Unlock()

	fc.totalFq++

	if _, ok := fc.values[v]; ok {
		fc.values[v]++
		if fc.values[v] == 1 {
			return v
		}
	} else {
		fc.values[v] = uint64(1)
		return v
	}
	return nil
}

// Removes decreases the count for v and returns it if the
// count reaches 0
func (fc *FreqCounter) Remove(v interface{}) interface{} {
	fc.valMu.Lock()
	defer fc.valMu.Unlock()

	if cnt, ok := fc.values[v]; ok {
		if cnt > 0 {
			fc.totalFq--
			fc.values[v]--
			if fc.values[v] == 0 {
				return v
			}
		}
	}
	return nil
}

func (fc *FreqCounter) Values() map[interface{}]uint64 {
	fc.valMu.Lock()
	defer fc.valMu.Unlock()

	return fc.values
}

func (fc *FreqCounter) TotalFreq() uint64 {
	return fc.totalFq
}

func (fc *FreqCounter) Reset() {
	for k := range fc.values {
		delete(fc.values, k)
	}
}
