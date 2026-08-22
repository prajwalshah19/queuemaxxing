package queue

import "time"

type queueClock interface {
	Now() time.Time
	NewTimer(time.Duration) queueTimer
}

type queueTimer interface {
	C() <-chan time.Time
	Stop() bool
}

type systemClock struct{}

func (systemClock) Now() time.Time { return time.Now() }

func (systemClock) NewTimer(d time.Duration) queueTimer {
	return systemTimer{Timer: time.NewTimer(d)}
}

type systemTimer struct {
	*time.Timer
}

func (t systemTimer) C() <-chan time.Time { return t.Timer.C }

type functionClock struct {
	now func() time.Time
}

func (c functionClock) Now() time.Time { return c.now() }

func (functionClock) NewTimer(d time.Duration) queueTimer {
	return systemTimer{Timer: time.NewTimer(d)}
}
