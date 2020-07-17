package stream_math

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPValue(t *testing.T) {
	x := []float64{1, 2, 3, 6, 8, 9, 45, 12}
	y := []float64{0, -1, 4, 7, 123, 23, 45, 2}

	assert.Equal(t, len(x), len(y))

	numElements := len(x)
	pval := NewPValue()

	for i := 0; i < numElements; i++ {
		pval.Add(x[i], y[i])
	}

	res, err := pval.Result()

	assert.Nil(t, err)
	//fmt.Println(res)

	assert.Equal(t, 0.5331177695581133, res)
}
