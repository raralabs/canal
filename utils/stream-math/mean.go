package stream_math

import "sync/atomic"

type Mean struct {
	lastMean float64
	num      uint64
}

func NewMean() *Mean {
	return &Mean{
		lastMean: 0,
		num:      0,
	}
}

func (m *Mean) Add(v float64) {
	atomic.AddUint64(&m.num, 1)
	m.lastMean = m.lastMean + (v-m.lastMean)/float64(m.num)
}

func (m *Mean) Result() (float64, error) {
	return m.lastMean, nil
}

func (m *Mean) Reset() {
	m.lastMean = 0
	m.num = 0
}
