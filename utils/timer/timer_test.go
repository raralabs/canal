package timer

import (
	"testing"
)

type dummy struct {
	val1   int
	val2   int
	result int
}

func (d *dummy) add() {
	d.result = d.val1 + d.val2
}

func TestTimer(t *testing.T) {

	// var timer *Timer
	// d1 := dummy{val1: 10, val2: 2, result: 0}
	// d2 := dummy{val1: 20, val2: 4, result: 0}

	// t.Run("Timer Creation", func(t *testing.T) {
	// 	timer = NewTimer(2*time.Millisecond, d1.add)

	// 	assert.NotNil(t, timer, "Timer has been created")
	// 	assert.Zero(t, d1.result, "No operation have been performed")

	// 	timer.StartTimer()
	// 	assert.Zero(t, d1.result, "No operation have been performed")

	// 	time.Sleep(5 * time.Millisecond)
	// 	time.Sleep(1 * time.Microsecond)

	// 	assert.Equal(t, 12, d1.result, "Addition have been performed")
	// 	timer.StopTimer()

	// 	d1.result = 0
	// 	time.Sleep(3 * time.Millisecond)
	// 	assert.Zero(t, d1.result, "No operation have been performed")

	// 	timer.ResetInterval(3 * time.Millisecond)
	// 	timer.StartTimer()

	// 	time.Sleep(1 * time.Millisecond)

	// 	assert.Equal(t, 0, d1.result, "Addition have not been performed")

	// 	time.Sleep(5 * time.Millisecond)
	// 	time.Sleep(1 * time.Microsecond)

	// 	assert.Equal(t, 12, d1.result, "Addition have been performed")
	// 	timer.StopTimer()
	// 	time.Sleep(10 * time.Millisecond)
	// })
}
