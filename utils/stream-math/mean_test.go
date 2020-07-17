package stream_math

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestMean(t *testing.T) {

	mean := NewMean()

	nums := []float64{1, 2, 3, 4, 5}

	for _, n := range nums {
		mean.Add(n)
	}

	res, _ := mean.Result()
	assert.Equal(t, float64(3), res)

	mean.Replace(1, 6)

	res, _ = mean.Result()
	assert.Equal(t, float64(4), res)

	mean.Remove(2)

	res, _ = mean.Result()
	assert.Equal(t, 4.5, res)

	mean.Remove(3)
	mean.Remove(4)
	mean.Remove(5)

	res, _ = mean.Result()
	assert.Equal(t, float64(6), res)

	mean.Remove(6)

	res, _ = mean.Result()
	assert.Equal(t, float64(0), res)
}
