package stream_math

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestVariance(t *testing.T) {
	data := []float64{2, 1, 3, 4, 5}

	variance := NewVariance()

	for _, d := range data {
		variance.Add(d)
	}

	res, _ := variance.Result()
	assert.Equal(t, float64(2), res)

	variance.Replace(1, 3)
	res, _ = variance.Result()
	assert.Equal(t, "1.04", fmt.Sprintf("%.2f", res))

	variance.Remove(3)
	res, _ = variance.Result()
	assert.Equal(t, "1.25", fmt.Sprintf("%.2f", res))
}
