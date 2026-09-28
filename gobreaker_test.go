package gobreaker

import (
	"errors"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

var defaultCB *CircuitBreaker
var customCB *CircuitBreaker

type StateChange struct {
	name string
	from State
	to   State
}

var stateChange StateChange

func pseudoSleep(cb *CircuitBreaker, period time.Duration) {
	if !cb.expiry.IsZero() {
		cb.expiry = cb.expiry.Add(-period)
	}
}

func succeed(cb *CircuitBreaker) error {
	_, err := cb.Execute(func() (any, error) { return nil, nil })
	return err
}

func succeedLater(cb *CircuitBreaker, delay time.Duration) <-chan error {
	ch := make(chan error)
	go func() {
		_, err := cb.Execute(func() (any, error) {
			time.Sleep(delay)
			return nil, nil
		})
		ch <- err
	}()
	return ch
}

func succeed2Step(cb *TwoStepCircuitBreaker) error {
	done, err := cb.Allow()
	if err != nil {
		return err
	}

	done(true)
	return nil
}

func fail(cb *CircuitBreaker) error {
	msg := "fail"
	_, err := cb.Execute(func() (any, error) { return nil, errors.New(msg) })
	if err.Error() == msg {
		return nil
	}

	return err
}

func fail2Step(cb *TwoStepCircuitBreaker) error {
	done, err := cb.Allow()
	if err != nil {
		return err
	}

	done(false)
	return nil
}

func causePanic(cb *CircuitBreaker) error {
	_, err := cb.Execute(func() (any, error) { panic("oops") })
	return err
}

func newCustom() *CircuitBreaker {
	var customSt Settings
	customSt.Name = "cb"
	customSt.MaxRequests = 3
	customSt.Interval = time.Duration(30) * time.Second
	customSt.Timeout = time.Duration(90) * time.Second
	customSt.ReadyToTrip = func(counts Counts) bool {
		numReqs := counts.Requests
		failureRatio := float64(counts.TotalFailures) / float64(numReqs)

		counts.clear()

		return numReqs >= 3 && failureRatio >= 0.6
	}
	customSt.OnStateChange = func(name string, from State, to State) {
		stateChange = StateChange{name, from, to}
	}

	return NewCircuitBreaker(customSt)
}

func newNegativeDurationCB() *CircuitBreaker {
	var negativeSt Settings
	negativeSt.Name = "ncb"
	negativeSt.Interval = time.Duration(-30) * time.Second
	negativeSt.Timeout = time.Duration(-90) * time.Second

	return NewCircuitBreaker(negativeSt)
}

func init() {
	defaultCB = NewCircuitBreaker(Settings{})
	customCB = newCustom()
}

func TestStateConstants(t *testing.T) {
	assert.Equal(t, State(0), StateClosed)
	assert.Equal(t, State(1), StateHalfOpen)
	assert.Equal(t, State(2), StateOpen)

	assert.Equal(t, StateClosed.String(), "closed")
	assert.Equal(t, StateHalfOpen.String(), "half-open")
	assert.Equal(t, StateOpen.String(), "open")
	assert.Equal(t, State(100).String(), "unknown state: 100")
}

func TestNewCircuitBreaker(t *testing.T) {
	defaultCB := NewCircuitBreaker(Settings{})
	assert.Equal(t, "", defaultCB.name)
	assert.Equal(t, uint32(1), defaultCB.maxRequests)
	assert.Equal(t, time.Duration(0), defaultCB.interval)
	assert.Equal(t, time.Duration(60)*time.Second, defaultCB.timeout)
	assert.NotNil(t, defaultCB.readyToTrip)
	assert.Nil(t, defaultCB.onStateChange)
	assert.Equal(t, StateClosed, defaultCB.state)
	assert.Equal(t, Counts{0, 0, 0, 0, 0}, defaultCB.counts)
	assert.True(t, defaultCB.expiry.IsZero())

	customCB := newCustom()
	assert.Equal(t, "cb", customCB.name)
	assert.Equal(t, uint32(3), customCB.maxRequests)
	assert.Equal(t, time.Duration(30)*time.Second, customCB.interval)
	assert.Equal(t, time.Duration(90)*time.Second, customCB.timeout)
	assert.NotNil(t, customCB.readyToTrip)
	assert.NotNil(t, customCB.onStateChange)
	assert.Equal(t, StateClosed, customCB.state)
	assert.Equal(t, Counts{0, 0, 0, 0, 0}, customCB.counts)
	assert.False(t, customCB.expiry.IsZero())

	negativeDurationCB := newNegativeDurationCB()
	assert.Equal(t, "ncb", negativeDurationCB.name)
	assert.Equal(t, uint32(1), negativeDurationCB.maxRequests)
	assert.Equal(t, time.Duration(0)*time.Second, negativeDurationCB.interval)
	assert.Equal(t, time.Duration(60)*time.Second, negativeDurationCB.timeout)
	assert.NotNil(t, negativeDurationCB.readyToTrip)
	assert.Nil(t, negativeDurationCB.onStateChange)
	assert.Equal(t, StateClosed, negativeDurationCB.state)
	assert.Equal(t, Counts{0, 0, 0, 0, 0}, negativeDurationCB.counts)
	assert.True(t, negativeDurationCB.expiry.IsZero())
}

func TestDefaultCircuitBreaker(t *testing.T) {
	assert.Equal(t, "", defaultCB.Name())

	for range 5 {
		assert.Nil(t, fail(defaultCB))
	}

	assert.Equal(t, StateClosed, defaultCB.State())
	assert.Equal(t, Counts{5, 0, 5, 0, 5}, defaultCB.counts)

	assert.Nil(t, succeed(defaultCB))
	assert.Equal(t, StateClosed, defaultCB.State())
	assert.Equal(t, Counts{6, 1, 5, 1, 0}, defaultCB.counts)

	assert.Nil(t, fail(defaultCB))
	assert.Equal(t, StateClosed, defaultCB.State())
	assert.Equal(t, Counts{7, 1, 6, 0, 1}, defaultCB.counts)

	for range 5 {
		assert.Nil(t, fail(defaultCB))
	}

	assert.Equal(t, StateOpen, defaultCB.State())
	assert.Equal(t, Counts{0, 0, 0, 0, 0}, defaultCB.counts)
	assert.False(t, defaultCB.expiry.IsZero())

	assert.Error(t, succeed(defaultCB))
	assert.Error(t, fail(defaultCB))
	assert.Equal(t, Counts{0, 0, 0, 0, 0}, defaultCB.counts)

	pseudoSleep(defaultCB, time.Duration(59)*time.Second)
	assert.Equal(t, StateOpen, defaultCB.State())

	pseudoSleep(defaultCB, time.Duration(1)*time.Second)
	assert.Equal(t, StateHalfOpen, defaultCB.State())
	assert.True(t, defaultCB.expiry.IsZero())

	assert.Nil(t, fail(defaultCB))
	assert.Equal(t, StateOpen, defaultCB.State())
	assert.Equal(t, Counts{0, 0, 0, 0, 0}, defaultCB.counts)
	assert.False(t, defaultCB.expiry.IsZero())

	pseudoSleep(defaultCB, time.Duration(60)*time.Second)
	assert.Equal(t, StateHalfOpen, defaultCB.State())
	assert.True(t, defaultCB.expiry.IsZero())

	assert.Nil(t, succeed(defaultCB))
	assert.Equal(t, StateClosed, defaultCB.State())
	assert.Equal(t, Counts{0, 0, 0, 0, 0}, defaultCB.counts)
	assert.True(t, defaultCB.expiry.IsZero())
}

func TestCustomCircuitBreaker(t *testing.T) {
	assert.Equal(t, "cb", customCB.Name())

	for range 5 {
		assert.Nil(t, succeed(customCB))
		assert.Nil(t, fail(customCB))
	}

	assert.Equal(t, StateClosed, customCB.State())
	assert.Equal(t, Counts{10, 5, 5, 0, 1}, customCB.counts)

	pseudoSleep(customCB, time.Duration(29)*time.Second)
	assert.Nil(t, succeed(customCB))
	assert.Equal(t, StateClosed, customCB.State())
	assert.Equal(t, Counts{11, 6, 5, 1, 0}, customCB.counts)

	pseudoSleep(customCB, time.Duration(1)*time.Second)
	assert.Nil(t, fail(customCB))
	assert.Equal(t, StateClosed, customCB.State())
	assert.Equal(t, Counts{1, 0, 1, 0, 1}, customCB.counts)

	assert.Nil(t, succeed(customCB))
	assert.Nil(t, fail(customCB))
	assert.Equal(t, StateOpen, customCB.State())
	assert.Equal(t, Counts{0, 0, 0, 0, 0}, customCB.counts)
	assert.False(t, customCB.expiry.IsZero())
	assert.Equal(t, StateChange{"cb", StateClosed, StateOpen}, stateChange)

	pseudoSleep(customCB, time.Duration(90)*time.Second)
	assert.Equal(t, StateHalfOpen, customCB.State())
	assert.True(t, defaultCB.expiry.IsZero())
	assert.Equal(t, StateChange{"cb", StateOpen, StateHalfOpen}, stateChange)

	assert.Nil(t, succeed(customCB))
	assert.Nil(t, succeed(customCB))
	assert.Equal(t, StateHalfOpen, customCB.State())
	assert.Equal(t, Counts{2, 2, 0, 2, 0}, customCB.counts)

	ch := succeedLater(customCB, time.Duration(100)*time.Millisecond)
	time.Sleep(time.Duration(50) * time.Millisecond)
	assert.Equal(t, Counts{3, 2, 0, 2, 0}, customCB.Counts())
	assert.Error(t, succeed(customCB))
	assert.Nil(t, <-ch)
	assert.Equal(t, StateClosed, customCB.State())
	assert.Equal(t, Counts{0, 0, 0, 0, 0}, customCB.counts)
	assert.False(t, customCB.expiry.IsZero())
	assert.Equal(t, StateChange{"cb", StateHalfOpen, StateClosed}, stateChange)
}

func TestTwoStepCircuitBreaker(t *testing.T) {
	tscb := NewTwoStepCircuitBreaker(Settings{Name: "tscb"})
	assert.Equal(t, "tscb", tscb.Name())

	for range 5 {
		assert.Nil(t, fail2Step(tscb))
	}

	assert.Equal(t, StateClosed, tscb.State())
	assert.Equal(t, Counts{5, 0, 5, 0, 5}, tscb.cb.counts)

	assert.Nil(t, succeed2Step(tscb))
	assert.Equal(t, StateClosed, tscb.State())
	assert.Equal(t, Counts{6, 1, 5, 1, 0}, tscb.cb.counts)

	assert.Nil(t, fail2Step(tscb))
	assert.Equal(t, StateClosed, tscb.State())
	assert.Equal(t, Counts{7, 1, 6, 0, 1}, tscb.cb.counts)

	for range 5 {
		assert.Nil(t, fail2Step(tscb))
	}

	assert.Equal(t, StateOpen, tscb.State())
	assert.Equal(t, Counts{0, 0, 0, 0, 0}, tscb.cb.counts)
	assert.False(t, tscb.cb.expiry.IsZero())

	assert.Error(t, succeed2Step(tscb))
	assert.Error(t, fail2Step(tscb))
	assert.Equal(t, Counts{0, 0, 0, 0, 0}, tscb.cb.counts)

	pseudoSleep(tscb.cb, time.Duration(59)*time.Second)
	assert.Equal(t, StateOpen, tscb.State())

	pseudoSleep(tscb.cb, time.Duration(1)*time.Second)
	assert.Equal(t, StateHalfOpen, tscb.State())
	assert.True(t, tscb.cb.expiry.IsZero())

	assert.Nil(t, fail2Step(tscb))
	assert.Equal(t, StateOpen, tscb.State())
	assert.Equal(t, Counts{0, 0, 0, 0, 0}, tscb.cb.counts)
	assert.False(t, tscb.cb.expiry.IsZero())

	pseudoSleep(tscb.cb, time.Duration(60)*time.Second)
	assert.Equal(t, StateHalfOpen, tscb.State())
	assert.True(t, tscb.cb.expiry.IsZero())

	assert.Nil(t, succeed2Step(tscb))
	assert.Equal(t, StateClosed, tscb.State())
	assert.Equal(t, Counts{0, 0, 0, 0, 0}, tscb.cb.counts)
	assert.True(t, tscb.cb.expiry.IsZero())
}

func TestPanicInRequest(t *testing.T) {
	assert.Panics(t, func() { _ = causePanic(defaultCB) })
	assert.Equal(t, Counts{1, 0, 1, 0, 1}, defaultCB.counts)
}

func TestGeneration(t *testing.T) {
	pseudoSleep(customCB, time.Duration(29)*time.Second)
	assert.Nil(t, succeed(customCB))
	ch := succeedLater(customCB, time.Duration(1500)*time.Millisecond)
	time.Sleep(time.Duration(500) * time.Millisecond)
	assert.Equal(t, Counts{2, 1, 0, 1, 0}, customCB.Counts())

	time.Sleep(time.Duration(500) * time.Millisecond)
	assert.Equal(t, StateClosed, customCB.State())
	assert.Equal(t, Counts{0, 0, 0, 0, 0}, customCB.Counts())

	assert.Nil(t, <-ch)
	assert.Equal(t, Counts{0, 0, 0, 0, 0}, customCB.Counts())
}

func TestCustomIsSuccessful(t *testing.T) {
	isSuccessful := func(error) bool {
		return true
	}
	cb := NewCircuitBreaker(Settings{IsSuccessful: isSuccessful})

	for range 5 {
		assert.Nil(t, fail(cb))
	}

	assert.Equal(t, StateClosed, cb.State())
	assert.Equal(t, Counts{5, 5, 0, 5, 0}, cb.counts)

	cb.counts.clear()

	cb.isSuccessful = func(err error) bool {
		return err == nil
	}
	for range 6 {
		assert.Nil(t, fail(cb))
	}

	assert.Equal(t, StateOpen, cb.State())

}

func TestCircuitBreakerInParallel(t *testing.T) {
	runtime.GOMAXPROCS(runtime.NumCPU())

	ch := make(chan error)

	const numReqs = 10000
	routine := func() {
		for range numReqs {
			ch <- succeed(customCB)
		}
	}

	const numRoutines = 10
	for range numRoutines {
		go routine()
	}

	total := uint32(numReqs * numRoutines)
	for range total {
		err := <-ch
		assert.Nil(t, err)
	}

	assert.Equal(t, Counts{total, total, 0, total, 0}, customCB.counts)
}
