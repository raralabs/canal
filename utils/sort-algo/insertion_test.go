package sort_algo

import (
	"github.com/raralabs/canal/utils/cast"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestInsertion(t *testing.T) {
	data := []float64{-12, 23, 4234, -12.312, 11.001}

	insertion := NewInsertion(func(old, new interface{}) bool {
		o, _ := cast.TryFloat(old)
		n, _ := cast.TryFloat(new)

		if o < n {
			return true
		}
		return false
	})

	for _, d := range data {
		insertion.Add(d)
	}

	expected := []float64{4234, 23, 11.001, -12, -12.312}
	e := insertion.First()
	for _, ex := range expected {
		if e == nil {
			break
		}
		assert.Equal(t, ex, e.Value)
		e = e.Next()
	}
}
