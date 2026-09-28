package gobreaker

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func succeed2Step(cb *TwoStepCircuitBreaker[bool]) error {
	done, err := cb.Allow()
	if err != nil {
		return err
	}

	done(nil)
	return nil
}

func fail2Step(cb *TwoStepCircuitBreaker[bool]) error {
	done, err := cb.Allow()
	if err != nil {
		return err
	}

	done(errFailed)
	return nil
}

func exclude2step(cb *TwoStepCircuitBreaker[bool]) error {
	done, err := cb.Allow()
	if err != nil {
		return err
	}

	done(errExcluded)
	return nil
}

func exclude2StepWithDelay(cb *TwoStepCircuitBreaker[bool]) (chan struct{}, error) {
	done, err := cb.Allow()
	if err != nil {
		return nil, err
	}

	finished := make(chan struct{})
	go func() {
		time.Sleep(100 * time.Millisecond)
		done(errExcluded)
		close(finished)
	}()

	return finished, nil
}

func TestTwoStepCircuitBreaker(t *testing.T) {
	tscb := NewTwoStepCircuitBreaker[bool](
		Settings{
			Name:        "tscb",
			MaxRequests: 2,
			IsExcluded: func(err error) bool {
				return errors.Is(err, errExcluded)
			},
		},
	)
	assert.Equal(t, "tscb", tscb.Name())

	for range 5 {
		assert.Nil(t, fail2Step(tscb))
	}

	assert.Equal(t, StateClosed, tscb.State())
	assert.Equal(t, Counts{Requests: 5, TotalFailures: 5, ConsecutiveFailures: 5}, tscb.cb.Counts())

	assert.Nil(t, succeed2Step(tscb))
	assert.Equal(t, StateClosed, tscb.State())
	assert.Equal(t, Counts{Requests: 6, TotalSuccesses: 1, TotalFailures: 5, ConsecutiveSuccesses: 1}, tscb.cb.Counts())

	assert.Nil(t, fail2Step(tscb))
	assert.Equal(t, StateClosed, tscb.State())
	assert.Equal(t, Counts{Requests: 7, TotalSuccesses: 1, TotalFailures: 6, ConsecutiveFailures: 1}, tscb.cb.Counts())

	for range 5 {
		assert.Nil(t, fail2Step(tscb))
	}

	assert.Equal(t, StateOpen, tscb.State())
	assert.Equal(t, Counts{}, tscb.cb.Counts())
	assert.False(t, tscb.cb.expiry.IsZero())

	assert.Error(t, succeed2Step(tscb))
	assert.Error(t, fail2Step(tscb))
	assert.Error(t, exclude2step(tscb))
	assert.Equal(t, Counts{}, tscb.cb.Counts())

	pseudoSleep(tscb.cb, tscb.cb.timeout-time.Millisecond)
	assert.Equal(t, StateOpen, tscb.State())

	pseudoSleep(tscb.cb, time.Second)
	assert.Equal(t, StateHalfOpen, tscb.State())
	assert.True(t, tscb.cb.expiry.IsZero())

	ch1, err := exclude2StepWithDelay(tscb)
	assert.Nil(t, err)
	ch2, err := exclude2StepWithDelay(tscb)
	assert.Nil(t, err)

	assert.Equal(t, ErrTooManyRequests, succeed2Step(tscb))
	assert.Equal(t, ErrTooManyRequests, fail2Step(tscb))

	<-ch1
	<-ch2

	assert.Nil(t, succeed2Step(tscb))

	assert.Nil(t, fail2Step(tscb))
	assert.Equal(t, StateOpen, tscb.State())
	assert.Equal(t, Counts{}, tscb.cb.Counts())
	assert.False(t, tscb.cb.expiry.IsZero())

	pseudoSleep(tscb.cb, tscb.cb.timeout+time.Nanosecond)
	assert.Equal(t, StateHalfOpen, tscb.State())
	assert.True(t, tscb.cb.expiry.IsZero())

	assert.Nil(t, succeed2Step(tscb))
	assert.Nil(t, succeed2Step(tscb))
	assert.Equal(t, StateClosed, tscb.State())
	assert.Equal(t, Counts{}, tscb.cb.Counts())
	assert.True(t, tscb.cb.expiry.IsZero())
}
