package stream_math

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestCovariance(t *testing.T) {
	xVals := []float64{2, 1, 3, 4, 5}
	yVals := []float64{2, -1, -3, -4, 15}

	variance := NewCovariance()

	for i := range xVals {
		variance.Add(xVals[i], yVals[i])
	}

	res, _ := variance.Result()
	assert.Equal(t, "5.20", fmt.Sprintf("%.2f", res))

	variance.Replace(2, 2, 1, 1)
	res, _ = variance.Result()
	assert.Equal(t, "5.52", fmt.Sprintf("%.2f", res))

	variance.Remove(5, 15)
	res, _ = variance.Result()
	assert.Equal(t, "-2.31", fmt.Sprintf("%.2f", res))
}
