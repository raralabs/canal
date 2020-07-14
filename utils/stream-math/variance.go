package stream_math

import (
	"errors"
	"sync/atomic"
)

func square(x float64) float64 {
	return x*x
}

// Calculates ** Population Variance **
type Variance struct {
	num      uint64  // The number of elements that has arrived
	lastMean float64 // Mean of the last data
	lastV    float64 // The v
}

func NewVariance() *Variance {
	return &Variance{
		lastMean: 0,
		num:      0,
		lastV:    0,
	}
}

func (m *Variance) Add(v float64) {

	atomic.AddUint64(&m.num, 1)
	newMean := m.lastMean + (v-m.lastMean)/float64(m.num)

	vk := m.lastV + (v-m.lastMean)*(v-newMean)

	m.lastMean = newMean
	m.lastV = vk
}

func (m *Variance) Replace(old, new float64) {
	if m.num == 0 {
		return
	}
	v := m.lastV / float64(m.num)
	mn := m.lastMean

	m_ := mn + (new - old) / float64(m.num)

	v_ := v + square(m_ - mn) + ( square(new - m_) - square(old - m_)) / float64(m.num)

	m.lastMean = m_
	m.lastV = v_ * float64(m.num)
}

func (m *Variance) Result() (float64, error) {
	if m.num > 0 {
		return m.lastV / float64(m.num), nil
	}
	return 0, errors.New("division by 0 during variance calculation")
}

func (m *Variance) Reset() {
	m.lastMean = 0
	m.num = 0
	m.lastV = 0
}
