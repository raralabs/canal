package stream_math

import "sync/atomic"

type Mean struct {
	mean float64
	num  uint64
}

func NewMean() *Mean {
	return &Mean{
		mean: 0,
		num:  0,
	}
}

func (m *Mean) Add(v float64) {
	atomic.AddUint64(&m.num, 1)
	m.mean = m.mean + (v-m.mean)/float64(m.num)
}

func (m *Mean) Replace(old, new float64) {
	m.mean += (new - old) / float64(m.num)
}

func (m *Mean) Result() (float64, error) {
	return m.mean, nil
}

func (m *Mean) Reset() {
	m.mean = 0
	m.num = 0
}