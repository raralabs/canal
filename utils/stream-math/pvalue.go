package stream_math

import (
	"errors"
	"math"
	"sync/atomic"

	"gonum.org/v1/gonum/stat/distuv"
)

// Source: https://en.wikipedia.org/wiki/Pearson_correlation_coefficient#Inference

type PValue struct {
	corr *Correlation
	num  uint64
}

func NewPValue() *PValue {
	return &PValue{
		corr: NewCorrelation(),
		num:  0,
	}
}

func (m *PValue) Add(x, y float64) {
	m.corr.Add(x, y)
	atomic.AddUint64(&m.num, 1)
}

func (m *PValue) Result() (float64, error) {
	if m.num < 3 {
		return 0, errors.New("pvalue calculation needs at least 2 sets of data")
	}
	r, err := m.corr.Result()
	if err != nil {
		return 0, err
	}
	return m.twoTail(r, float64(m.num)), nil
}

func (m *PValue) Reset() {
	m.corr.Reset()
	m.num = 0
}

func (m *PValue) twoTail(r float64, n float64) float64 {

	// compute the test stat
	ts := r * math.Sqrt((n-2)/(1-r*r))

	// make a Student's t with (n-2) d.f.
	t := distuv.StudentsT{Mu: 0, Sigma: 1, Nu: n - 2, Src: nil}

	// compute the p-value
	pval := 2 * t.CDF(-math.Abs(ts))

	return pval
}
