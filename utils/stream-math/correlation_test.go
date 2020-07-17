package stream_math

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStats(t *testing.T) {
	x := []float64{1, 2, 3, 6, 8, 9, 45, 12}
	y := []float64{0, -1, 4, 7, 123, 23, 45, 2}

	assert.Equal(t, len(x), len(y))

	numElements := len(x)

	meanX := NewMean()
	meanY := NewMean()
	varX := NewVariance()
	varY := NewVariance()
	cov := NewCovariance()
	corr := NewCorrelation()

	for i := 0; i < numElements; i++ {
		meanX.Add(x[i])
		meanY.Add(y[i])
		varX.Add(x[i])
		varY.Add(y[i])
		cov.Add(x[i], y[i])
		corr.Add(x[i], y[i])
	}

	mX, _ := meanX.Result()
	mY, _ := meanY.Result()
	vX, _ := varX.Result()
	vY, _ := varY.Result()
	cv, _ := cov.Result()
	cr, _ := corr.Result()

	//fmt.Printf("Mean X: %v\n", mX)
	//fmt.Printf("Mean Y: %v\n", mY)
	//fmt.Printf("Var  X: %v\n", vX)
	//fmt.Printf("Var  Y: %v\n", vY)
	//fmt.Printf("Cov   : %v\n", cv)
	//fmt.Printf("Corr  : %v\n", cr)

	t.Run("Test MeanX", func(t *testing.T) {
		assert.Equal(t, 10.75, mX)
	})

	t.Run("Test MeanY", func(t *testing.T) {
		assert.Equal(t, 25.375, mY)
	})

	t.Run("Test VarianceX", func(t *testing.T) {
		assert.Equal(t, 179.93749999999997, vX)
	})

	t.Run("Test VarianceY", func(t *testing.T) {
		assert.Equal(t, 1575.2343750000002, vY)
	})

	t.Run("Test Covariance", func(t *testing.T) {
		assert.Equal(t, 138.71875, cv)
	})

	t.Run("Test Correlation", func(t *testing.T) {
		assert.Equal(t, 0.2605563941366789, cr)
	})
}
