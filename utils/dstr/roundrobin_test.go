package dstr

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestRoundRobin(t *testing.T) {

	t.Run("Single Insert", func(t *testing.T) {
		rb := NewRoundRobin(5)
		assert.Equal(t, uint64(5), rb.Cap())
		assert.Equal(t, uint64(0), rb.Len())

		err := rb.Put(5)
		if !assert.Nil(t, err) {
			return
		}

		result, err := rb.GetAll()
		if !assert.Nil(t, err) {
			return
		}

		assert.Equal(t, 5, result[0])
		val, _ := rb.Get(0)
		assert.Equal(t, 5, val, "")
	})

	t.Run("Multiple Insert", func(t *testing.T) {
		rb := NewRoundRobin(5)

		err := rb.Put(1)
		if !assert.Nil(t, err) {
			return
		}

		err = rb.Put(2)
		if !assert.Nil(t, err) {
			return
		}

		result, err := rb.GetAll()
		if !assert.Nil(t, err) {
			return
		}

		assert.Equal(t, 1, result[0])
		assert.Equal(t, 2, result[1])
	})

	t.Run("PutToFull", func(t *testing.T) {
		rb := NewRoundRobin(3)

		for i := 0; i < 10; i++ {
			err := rb.Put(i)
			val, _ := rb.GetLast()
			assert.Equal(t, i, val, "")
			if !assert.Nil(t, err) {
				return
			}
		}

		result, err := rb.GetAll()
		assert.Len(t, result, 3)
		if !assert.Nil(t, err) {
			return
		}

		assert.Equal(t, 7, result[0])
		assert.Equal(t, 8, result[1])
		assert.Equal(t, 9, result[2])

		val, _ := rb.Get(0)
		assert.Equal(t, 7, val, "")
		val, _ = rb.Get(1)
		assert.Equal(t, 8, val, "")
		val, _ = rb.Get(2)
		assert.Equal(t, 9, val, "")

		val, _ = rb.GetLast()
		assert.Equal(t, 9, val, "")

		val, _ = rb.Pop()
		assert.Equal(t, 9, val, "")

		val, _ = rb.Pop()
		assert.Equal(t, 8, val, "")

		val, _ = rb.Pop()
		assert.Equal(t, 7, val, "")

		rb.Dispose()
		res, err := rb.GetAll()
		assert.NotNil(t, err)
		assert.Nil(t, res)

		err = rb.Put(10)
		assert.NotNil(t, err)
	})

}
