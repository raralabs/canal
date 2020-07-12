package stream_math

import (
	"errors"
	"math"
)

// Source: https://www.probabilitycourse.com/chapter5/5_3_1_covariance_correlation.php

type Correlation struct {
	covXY *Covariance
	varX  *Variance
	varY  *Variance
}

func NewCorrelation() *Correlation {
	return &Correlation{
		covXY: NewCovariance(),
		varX:  NewVariance(),
		varY:  NewVariance(),
	}
}

func (m *Correlation) Add(x, y float64) {
	m.covXY.Add(x, y)
	m.varX.Add(x)
	m.varY.Add(y)
}

func (m *Correlation) Result() (float64, error) {
	vX, err := m.varX.Result()
	if err != nil {
		return 0, err
	}
	vY, err := m.varY.Result()
	if err != nil {
		return 0, err
	}

	covXY, _ := m.covXY.Result()
	denom := math.Sqrt(vX * vY)

	if denom == 0 {
		return 0, errors.New("division by 0 during correlation calculation")
	}

	corr := covXY / denom

	if corr < -1 || corr > 1 {
		return 0, errors.New("error in correlation calculation")
	}

	return corr, nil
}

func (m *Correlation) Reset() {
	m.varX.Reset()
	m.varY.Reset()
	m.covXY.Reset()
}
