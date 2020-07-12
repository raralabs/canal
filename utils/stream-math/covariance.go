package stream_math

import (
	"errors"
	"sync/atomic"
)

// Source: Online section of https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Covariance
// Calculates ** Population Covariance **
type Covariance struct {
	mX, mY float64
	C      float64
	n      uint64
}

func NewCovariance() *Covariance {

	cov := &Covariance{}
	cov.Reset()

	return cov
}

func (m *Covariance) Add(x, y float64) {
	atomic.AddUint64(&m.n, 1)
	dx := x - m.mX
	m.mX += dx / float64(m.n)
	m.mY += (y - m.mY) / float64(m.n)
	m.C += dx * (y - m.mY)
}

func (m *Covariance) Result() (float64, error) {
	if m.n == 0 {
		return 0, errors.New("division by 0 during covariance calculation")
	}
	return m.C / float64(m.n), nil
}

func (m *Covariance) Reset() {
	m.mX = 0
	m.mY = 0
	m.C = 0
	m.n = 0
}
