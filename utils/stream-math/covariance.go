package stream_math

// Source: Online section of https://www.probabilitycourse.com/chapter5/5_3_1_covariance_correlation.php

type Covariance struct {
	mX, mY, mXY *Mean
}

func NewCovariance() *Covariance {

	cov := &Covariance{
		mX:  NewMean(),
		mY:  NewMean(),
		mXY: NewMean(),
	}
	cov.Reset()

	return cov
}

func (m *Covariance) Add(x, y float64) {
	m.mX.Add(x)
	m.mY.Add(y)
	m.mXY.Add(x * y)
}

func (m *Covariance) Remove(x, y float64) {
	m.mX.Remove(x)
	m.mY.Remove(y)
	m.mXY.Remove(x * y)
}

func (m *Covariance) Replace(x1, y1, x2, y2 float64) {
	m.mX.Replace(x1, x2)
	m.mY.Replace(y1, y2)
	m.mXY.Replace(x1*y1, x2*y2)
}

func (m *Covariance) Result() (float64, error) {
	mx, _ := m.mX.Result()
	my, _ := m.mY.Result()
	mxy, _ := m.mXY.Result()

	return mxy - mx*my, nil
}

func (m *Covariance) Reset() {
	m.mX.Reset()
	m.mY.Reset()
	m.mXY.Reset()
}
