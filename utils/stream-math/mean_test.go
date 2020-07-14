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
}
