package timer

import (
	"sync"
	"time"
)

// A Timer is an entity that can call an event(callback function) on a specified
// interval of time.
type Timer struct {
	timerCallback TimerCallback
	interval      time.Duration
	timerRunning  bool
	mutex         *sync.Mutex
	done          chan bool
}

// NewTimer creates a new timer with the given interval and callback function.
func NewTimer(interval time.Duration, callback TimerCallback) *Timer {

	t := &Timer{}

	t.timerCallback = callback
	t.interval = interval
	t.timerRunning = false
	t.mutex = &sync.Mutex{}

	return t
}

// ResetInterval resets the interval of the timer.
// The timer is stopped after the call to this function.
// The timer need to be started again if it was started before.
func (t *Timer) ResetInterval(interval time.Duration) {
	t.StopTimer()

	t.mutex.Lock()
	t.interval = interval
	t.mutex.Unlock()
}

// The timer is running if it has been started and not stopped.
func (t *Timer) TimerRunning() bool {
	return t.timerRunning
}

// StartTimer starts the timer if it is not already running.
func (t *Timer) StartTimer() {
	if !(t.TimerRunning()) {

		t.mutex.Lock()
		t.timerRunning = true
		t.mutex.Unlock()

		t.done = t.startTicker()
	}
}

// StopTimer stops the timer if it running.
func (t *Timer) StopTimer() {
	if t.TimerRunning() {
		t.mutex.Lock()
		t.timerRunning = false
		close(t.done)
		t.mutex.Unlock()
	}
}

// startTicker starts the timer ticker.
func (t *Timer) startTicker() chan bool {
	done := make(chan bool, 1)

	go func() {
		lengthTicker := time.NewTicker(t.interval)
		defer lengthTicker.Stop()
		for {
			select {
			case <-lengthTicker.C:
				t.timerCallback()

			case <-done:
				// fmt.Println("Timer Stopped")
				return
			}
		}
	}()

	return done
}
