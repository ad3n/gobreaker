package gobreaker

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type MockStore struct {
	data map[string][]byte
	mu   sync.RWMutex
}

func NewMockStore() *MockStore {
	return &MockStore{
		data: make(map[string][]byte),
	}
}

func (m *MockStore) Lock(name string) error {

	return nil
}

func (m *MockStore) Unlock(name string) error {

	return nil
}

func (m *MockStore) GetData(name string) ([]byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	data, exists := m.data[name]
	if !exists {
		return nil, nil
	}

	return data, nil
}

func (m *MockStore) SetData(name string, data []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.data[name] = data
	return nil
}

func (m *MockStore) Close() {

}

func setUpDCB() *DistributedCircuitBreaker[any] {
	mockStore := NewMockStore()
	dcb, err := NewDistributedCircuitBreaker[any](mockStore, Settings{
		Name:        "TestBreaker",
		MaxRequests: 3,
		Interval:    time.Second,
		Timeout:     time.Second * 2,
		ReadyToTrip: func(counts Counts) bool {
			return counts.ConsecutiveFailures > 5
		},
	})
	if err != nil {
		panic(err)
	}

	return dcb
}

func dcbPseudoSleep(dcb *DistributedCircuitBreaker[any], period time.Duration) {
	state, err := dcb.getSharedState()
	if err != nil {
		panic(err)
	}

	state.Expiry = state.Expiry.Add(-period)

	if time.Now().After(state.Expiry) {
		state.Counts.clear()
		for i := range state.Buckets {
			state.Buckets[i].clear()
		}
	}

	err = dcb.setSharedState(state)
	if err != nil {
		panic(err)
	}
}

func successRequest(dcb *DistributedCircuitBreaker[any]) error {
	_, err := dcb.Execute(func() (any, error) { return nil, nil })
	return err
}

func failRequest(dcb *DistributedCircuitBreaker[any]) error {
	_, err := dcb.Execute(func() (any, error) { return nil, errors.New("fail") })
	if err != nil && err.Error() == "fail" {
		return nil
	}

	return err
}

func assertState(t *testing.T, dcb *DistributedCircuitBreaker[any], expected State) {
	state, err := dcb.State()
	assert.Equal(t, expected, state)
	assert.NoError(t, err)
}

func TestDistributedCircuitBreakerInitialization(t *testing.T) {
	dcb := setUpDCB()

	assert.Equal(t, "TestBreaker", dcb.Name())
	assert.Equal(t, uint32(3), dcb.maxRequests)
	assert.Equal(t, time.Second, dcb.interval)
	assert.Equal(t, time.Second*2, dcb.timeout)
	assert.NotNil(t, dcb.readyToTrip)

	assertState(t, dcb, StateClosed)
}

func TestDistributedCircuitBreakerStateTransitions(t *testing.T) {
	dcb := setUpDCB()

	assertState(t, dcb, StateClosed)

	for range 6 {
		assert.NoError(t, failRequest(dcb))
	}

	assertState(t, dcb, StateOpen)

	err := failRequest(dcb)
	assert.Equal(t, ErrOpenState, err)

	dcbPseudoSleep(dcb, dcb.timeout+time.Nanosecond)
	assertState(t, dcb, StateHalfOpen)

	for i := 0; i < int(dcb.maxRequests); i++ {
		assert.NoError(t, successRequest(dcb))
	}

	assertState(t, dcb, StateClosed)

	for range 6 {
		assert.NoError(t, failRequest(dcb))
	}

	assertState(t, dcb, StateOpen)
}

func TestDistributedCircuitBreakerExecution(t *testing.T) {
	dcb := setUpDCB()

	result, err := dcb.Execute(func() (any, error) {
		return "success", nil
	})
	assert.NoError(t, err)
	assert.Equal(t, "success", result)

	_, err = dcb.Execute(func() (any, error) {
		return nil, errors.New("test error")
	})
	assert.Error(t, err)
	assert.Equal(t, "test error", err.Error())
}

func TestDistributedCircuitBreakerCounts(t *testing.T) {
	dcb := setUpDCB()

	for range 5 {
		assert.Nil(t, successRequest(dcb))
	}

	state, err := dcb.getSharedState()
	assert.Equal(t, Counts{Requests: 5, TotalSuccesses: 5, ConsecutiveSuccesses: 5}, state.Counts)
	assert.NoError(t, err)

	assert.Nil(t, failRequest(dcb))
	state, err = dcb.getSharedState()
	assert.Equal(t, Counts{Requests: 6, TotalSuccesses: 5, TotalFailures: 1, ConsecutiveFailures: 1}, state.Counts)
	assert.NoError(t, err)
}

func TestCustomDistributedCircuitBreaker(t *testing.T) {
	mockStore := NewMockStore()
	customDCB, err := NewDistributedCircuitBreaker[any](mockStore, Settings{
		Name:        "CustomBreaker",
		MaxRequests: 3,
		Interval:    time.Second * 30,
		Timeout:     time.Second * 90,
		ReadyToTrip: func(counts Counts) bool {
			numReqs := counts.Requests
			failureRatio := float64(counts.TotalFailures) / float64(numReqs)
			return numReqs >= 3 && failureRatio >= 0.6
		},
	})
	assert.NoError(t, err)

	t.Run("Initialization", func(t *testing.T) {
		assert.Equal(t, "CustomBreaker", customDCB.Name())
		assertState(t, customDCB, StateClosed)
	})

	t.Run("Counts and State Transitions", func(t *testing.T) {

		for range 5 {
			assert.NoError(t, successRequest(customDCB))
			assert.NoError(t, failRequest(customDCB))
		}

		state, err := customDCB.getSharedState()
		assert.NoError(t, err)
		assert.Equal(t, StateClosed, state.State)
		assert.Equal(t, Counts{Requests: 10, TotalSuccesses: 5, TotalFailures: 5, ConsecutiveFailures: 1}, state.Counts)

		assert.NoError(t, successRequest(customDCB))
		state, err = customDCB.getSharedState()
		assert.NoError(t, err)
		assert.Equal(t, Counts{Requests: 11, TotalSuccesses: 6, TotalFailures: 5, ConsecutiveSuccesses: 1}, state.Counts)

		dcbPseudoSleep(customDCB, customDCB.interval+time.Nanosecond)

		assert.NoError(t, successRequest(customDCB))
		assert.NoError(t, failRequest(customDCB))
		assert.NoError(t, failRequest(customDCB))

		assertState(t, customDCB, StateOpen)

		state, err = customDCB.getSharedState()
		assert.NoError(t, err)
		assert.Equal(t, Counts{}, state.Counts)
	})

	t.Run("Timeout and Half-Open State", func(t *testing.T) {

		dcbPseudoSleep(customDCB, customDCB.timeout+time.Nanosecond)
		assertState(t, customDCB, StateHalfOpen)

		for range 3 {
			assert.NoError(t, successRequest(customDCB))
		}

		assertState(t, customDCB, StateClosed)
	})
}

func TestCustomDistributedCircuitBreakerStateTransitions(t *testing.T) {

	var stateChange StateChange
	customSt := Settings{
		Name:        "cb",
		MaxRequests: 3,
		Interval:    5 * time.Second,
		Timeout:     5 * time.Second,
		ReadyToTrip: func(counts Counts) bool {
			return counts.ConsecutiveFailures >= 2
		},
		OnStateChange: func(name string, from State, to State) {
			stateChange = StateChange{name, from, to}
		},
	}

	mockStore := NewMockStore()
	dcb, err := NewDistributedCircuitBreaker[any](mockStore, customSt)
	assert.NoError(t, err)

	t.Run("Circuit Breaker State Transitions", func(t *testing.T) {

		assertState(t, dcb, StateClosed)

		for range 2 {
			err := failRequest(dcb)
			assert.NoError(t, err, "Fail request should not return an error")
		}

		assertState(t, dcb, StateOpen)
		assert.Equal(t, StateChange{"cb", StateClosed, StateOpen}, stateChange)

		err := successRequest(dcb)
		assert.Error(t, err)
		assert.Equal(t, ErrOpenState, err)

		dcbPseudoSleep(dcb, dcb.timeout+time.Nanosecond)
		assertState(t, dcb, StateHalfOpen)
		assert.Equal(t, StateChange{"cb", StateOpen, StateHalfOpen}, stateChange)

		for i := 0; i < int(dcb.maxRequests); i++ {
			err := successRequest(dcb)
			assert.NoError(t, err)
		}

		assertState(t, dcb, StateClosed)
		assert.Equal(t, StateChange{"cb", StateHalfOpen, StateClosed}, stateChange)
	})
}

func TestDistributedCircuitBreakerTimeSynchronization(t *testing.T) {

	mockStore := NewMockStore()

	dcb1, err := NewDistributedCircuitBreaker[any](mockStore, Settings{
		Name:         "TimeSyncTest",
		MaxRequests:  3,
		Interval:     10 * time.Second,
		BucketPeriod: 2 * time.Second,
		Timeout:      5 * time.Second,
		ReadyToTrip: func(counts Counts) bool {
			return counts.ConsecutiveFailures >= 2
		},
	})
	assert.NoError(t, err)

	sharedState1, err := dcb1.getSharedState()
	assert.NoError(t, err)
	originalStart := sharedState1.Start

	time.Sleep(100 * time.Millisecond)
	_, err = dcb1.Execute(func() (any, error) {
		return "success", nil
	})
	assert.NoError(t, err)

	dcb2, err := NewDistributedCircuitBreaker[any](mockStore, Settings{
		Name:         "TimeSyncTest",
		MaxRequests:  3,
		Interval:     10 * time.Second,
		BucketPeriod: 2 * time.Second,
		Timeout:      5 * time.Second,
		ReadyToTrip: func(counts Counts) bool {
			return counts.ConsecutiveFailures >= 2
		},
	})
	assert.NoError(t, err)

	sharedState2, err := dcb2.getSharedState()
	assert.NoError(t, err)
	assert.Equal(t, originalStart, sharedState2.Start)

	_, err = dcb2.Execute(func() (any, error) {
		return "success", nil
	})
	assert.NoError(t, err)

	state1, err := dcb1.getSharedState()
	assert.NoError(t, err)
	state2, err := dcb2.getSharedState()
	assert.NoError(t, err)

	assert.Equal(t, state1.Age, state2.Age, "Both instances should have the same age")
	assert.Equal(t, state1.Counts, state2.Counts, "Both instances should have the same counts")
	assert.Equal(t, state1.Start, state2.Start, "Both instances should have the same start time")

	now := time.Now()
	expectedAge := uint64(now.Sub(originalStart) / (2 * time.Second))

	var minAge uint64
	if expectedAge > 0 {
		minAge = expectedAge - 1
	}

	assert.True(t, state1.Age >= minAge && state1.Age <= expectedAge+1,
		"Age should be calculated from shared start time, got %d, expected around %d", state1.Age, expectedAge)
}

func TestDistributedCircuitBreakerBucketIndexingConsistency(t *testing.T) {

	mockStore := NewMockStore()

	dcb1, err := NewDistributedCircuitBreaker[any](mockStore, Settings{
		Name:         "BucketTest",
		MaxRequests:  3,
		Interval:     6 * time.Second,
		BucketPeriod: 2 * time.Second,
		Timeout:      5 * time.Second,
		ReadyToTrip: func(counts Counts) bool {
			return counts.ConsecutiveFailures >= 2
		},
	})
	assert.NoError(t, err)

	_, err = dcb1.Execute(func() (any, error) {
		return "success", nil
	})
	assert.NoError(t, err)

	sharedState, err := dcb1.getSharedState()
	assert.NoError(t, err)
	sharedStart := sharedState.Start

	time.Sleep(2 * time.Second)

	dcb2, err := NewDistributedCircuitBreaker[any](mockStore, Settings{
		Name:         "BucketTest",
		MaxRequests:  3,
		Interval:     6 * time.Second,
		BucketPeriod: 2 * time.Second,
		Timeout:      5 * time.Second,
		ReadyToTrip: func(counts Counts) bool {
			return counts.ConsecutiveFailures >= 2
		},
	})
	assert.NoError(t, err)

	_, err = dcb1.Execute(func() (any, error) {
		return "success", nil
	})
	assert.NoError(t, err)

	_, err = dcb2.Execute(func() (any, error) {
		return "success", nil
	})
	assert.NoError(t, err)

	state1, err := dcb1.getSharedState()
	assert.NoError(t, err)
	state2, err := dcb2.getSharedState()
	assert.NoError(t, err)

	assert.Equal(t, state1.Age, state2.Age, "Bucket ages should be consistent")
	assert.Equal(t, state1.Start, state2.Start, "Start times should be synchronized")
	assert.Equal(t, state1.Counts, state2.Counts, "Counts should be consistent")

	now := time.Now()
	expectedAge := uint64(now.Sub(sharedStart) / (2 * time.Second))
	assert.True(t, state1.Age >= expectedAge-1 && state1.Age <= expectedAge+1,
		"Age should be calculated from shared start time")
}
