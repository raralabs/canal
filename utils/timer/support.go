package timer

import "time"

// TimerCallback is the callback function footprint of the timer.
type TimerCallback func()

// TimerSupport is used to create a timer on behalf of the activity.
type TimerSupport interface {
	// TimerRunning indicates if a timer is running.
	TimerRunning() bool

	// StartTimer starts the timer if it is not running.
	StartTimer()

	// CancelTimer cancels the existing timer.
	StopTimer()

	// ResetInterval resets the interval of the timer.
	ResetInterval(interval time.Duration)
}
