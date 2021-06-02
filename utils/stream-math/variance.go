package stream_math

import (
	"errors"
	"sync/atomic"
)

func square(x float64) float64 {
	return x * x
}

// Variance calculates the population variance
type Variance struct {
	num      uint64  // The number of elements that has arrived
	mean     float64 // Mean of the last data
	variance float64 // The v
}

func NewVariance() *Variance {
	return &Variance{
		mean:     0,
		num:      0,
		variance: 0,
	}
}

func (m *Variance) Add(v float64) {

	atomic.AddUint64(&m.num, 1)
	newMean := m.mean + (v-m.mean)/float64(m.num)

	m.variance = float64(m.num-1) * (m.variance + square(m.mean-v)/float64(m.num)) / float64(m.num)
	m.mean = newMean
}

func (m *Variance) Remove(v float64) {
	if m.num > 0 {
		m.num--
		if m.num == 0 {
			m.Reset()
		} else {
			newMean := (m.mean*float64(m.num+1) - v) / float64(m.num)
			m.variance = (float64(m.num+1)*m.variance)/float64(m.num) - square(newMean-v)/float64(m.num+1)
			m.mean = newMean
		}
	}
}

func (m *Variance) Replace(old, new float64) {
	if m.num == 0 {
		return
	}
	v := m.variance
	mn := m.mean

	m_ := mn + (new-old)/float64(m.num)

	v_ := v + square(m_-mn) + (square(new-m_)-square(old-m_))/float64(m.num)

	m.mean = m_
	m.variance = v_
}

func (m *Variance) Result() (float64, error) {
	if m.num > 0 {
		return m.variance, nil
	}
	return 0, errors.New("division by 0 during variance calculation")
}

func (m *Variance) Reset() {
	m.mean = 0
	m.num = 0
	m.variance = 0
}
