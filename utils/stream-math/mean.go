package stream_math

import (
	"github.com/raralabs/canal/utils/dstr"
	"sync/atomic"
)

type Mean struct {
	means *dstr.RoundRobin
	bufSz uint64
	num   uint64
}

func NewMean(bufSz uint64) *Mean {
	return &Mean{
		means: dstr.NewRoundRobin(bufSz),
		num:   0,
	}
}

func (m *Mean) Add(v float64) {
	mean := float64(0)
	if m.num > 0 {
		lst, _ := m.means.GetLast()
		mean, _ = lst.(float64)
	}
	atomic.AddUint64(&m.num, 1)
	mean = mean + (v-mean)/float64(m.num)
	m.means.Put(mean)
}

func (m *Mean) Pop() {
	m.num--
	m.means.Pop()
}

func (m *Mean) Result() (float64, error) {
	v, _ := m.means.GetLast()
	val, _ := v.(float64)
	return val, nil
}

func (m *Mean) Reset() {
	m.means.Dispose()
	m.means = dstr.NewRoundRobin(m.bufSz)
	m.num = 0
}
