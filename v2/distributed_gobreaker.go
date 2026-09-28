package gobreaker

import (
	"encoding/json"
	"errors"
	"sync"
	"time"
)

var (
	ErrNoSharedStore = errors.New("no shared store")

	ErrNoSharedState = errors.New("no shared state")
)

type SharedState struct {
	State      State     `json:"state"`
	Generation uint64    `json:"generation"`
	Age        uint64    `json:"age"`
	Counts     Counts    `json:"counts"`
	Buckets    []Counts  `json:"buckets"`
	Start      time.Time `json:"start"`
	Expiry     time.Time `json:"expiry"`
}

const maxPooledBuckets = 1024

var sharedStatePool = sync.Pool{
	New: func() any { return new(SharedState) },
}

func releaseSharedState(state *SharedState) {
	resetSharedState(state)
	sharedStatePool.Put(state)
}

func resetSharedState(state *SharedState) {
	buckets := state.Buckets
	if cap(buckets) > maxPooledBuckets {
		*state = SharedState{}
		return
	}

	clear(buckets[:cap(buckets)])
	*state = SharedState{Buckets: buckets[:0]}
}

type SharedDataStore interface {
	Lock(name string) error
	Unlock(name string) error
	GetData(name string) ([]byte, error)
	SetData(name string, data []byte) error
}

type DistributedCircuitBreaker[T any] struct {
	*CircuitBreaker[T]
	store    SharedDataStore
	lockKey  string
	stateKey string
}

func NewDistributedCircuitBreaker[T any](store SharedDataStore, settings Settings) (dcb *DistributedCircuitBreaker[T], err error) {
	if store == nil {
		return nil, ErrNoSharedStore
	}

	dcb = &DistributedCircuitBreaker[T]{
		CircuitBreaker: NewCircuitBreaker[T](settings),
		store:          store,
		lockKey:        "gobreaker:mutex:" + settings.Name,
		stateKey:       "gobreaker:state:" + settings.Name,
	}

	err = dcb.lock()
	if err != nil {
		return nil, err
	}
	defer func(dcb *DistributedCircuitBreaker[T]) {
		e := dcb.unlock()
		if err == nil {
			err = e
		}
	}(dcb)

	shared := sharedStatePool.Get().(*SharedState)
	defer releaseSharedState(shared)

	err = dcb.readSharedState(shared)
	if err == ErrNoSharedState {
		dcb.extractInto(shared)
		err = dcb.writeSharedState(shared)
	}

	if err != nil {
		return nil, err
	}

	return dcb, nil
}

const (
	mutexTimeout  = 5 * time.Second
	mutexWaitTime = 500 * time.Millisecond
)

func (dcb *DistributedCircuitBreaker[T]) mutexKey() string {
	const prefix = "gobreaker:mutex:"
	if len(dcb.lockKey) >= len(prefix) && dcb.lockKey[len(prefix):] == dcb.name {
		return dcb.lockKey
	}

	return prefix + dcb.name
}

func (dcb *DistributedCircuitBreaker[T]) lock() error {
	if dcb.store == nil {
		return ErrNoSharedStore
	}

	var err error
	expiry := time.Now().Add(mutexTimeout)
	for time.Now().Before(expiry) {
		err = dcb.store.Lock(dcb.mutexKey())
		if err == nil {
			return nil
		}

		time.Sleep(mutexWaitTime)
	}

	return err
}

func (dcb *DistributedCircuitBreaker[T]) unlock() error {
	if dcb.store == nil {
		return ErrNoSharedStore
	}

	return dcb.store.Unlock(dcb.mutexKey())
}

func (dcb *DistributedCircuitBreaker[T]) sharedStateKey() string {
	const prefix = "gobreaker:state:"
	if len(dcb.stateKey) >= len(prefix) && dcb.stateKey[len(prefix):] == dcb.name {
		return dcb.stateKey
	}

	return prefix + dcb.name
}

func (dcb *DistributedCircuitBreaker[T]) getSharedState() (SharedState, error) {
	var state SharedState
	err := dcb.readSharedState(&state)
	return state, err
}

func (dcb *DistributedCircuitBreaker[T]) readSharedState(state *SharedState) error {
	if dcb.store == nil {
		return ErrNoSharedStore
	}

	data, err := dcb.store.GetData(dcb.sharedStateKey())
	if len(data) == 0 {
		return ErrNoSharedState
	}

	if err != nil {
		return err
	}

	return json.Unmarshal(data, state)
}

func (dcb *DistributedCircuitBreaker[T]) setSharedState(state SharedState) error {
	return dcb.writeSharedState(&state)
}

func (dcb *DistributedCircuitBreaker[T]) writeSharedState(state *SharedState) error {
	if dcb.store == nil {
		return ErrNoSharedStore
	}

	data, err := marshalSharedState(state)
	if err != nil {
		return err
	}

	return dcb.store.SetData(dcb.sharedStateKey(), data)
}

func (dcb *DistributedCircuitBreaker[T]) inject(shared SharedState) {
	dcb.mutex.Lock()
	defer dcb.mutex.Unlock()

	dcb.state = shared.State
	dcb.generation = shared.Generation
	dcb.counts.Counts = shared.Counts
	dcb.counts.age = shared.Age
	if cap(dcb.counts.buckets) < len(shared.Buckets) {
		dcb.counts.buckets = make([]Counts, len(shared.Buckets))
	}

	dcb.counts.buckets = dcb.counts.buckets[:len(shared.Buckets)]
	copy(dcb.counts.buckets, shared.Buckets)
	dcb.start = shared.Start
	dcb.expiry = shared.Expiry
}

func (dcb *DistributedCircuitBreaker[T]) extract() SharedState {
	var state SharedState
	dcb.extractInto(&state)
	return state
}

func (dcb *DistributedCircuitBreaker[T]) extractInto(state *SharedState) {
	dcb.mutex.Lock()
	defer dcb.mutex.Unlock()

	buckets := state.Buckets
	if buckets == nil || cap(buckets) < len(dcb.counts.buckets) {
		buckets = make([]Counts, len(dcb.counts.buckets))
	}

	buckets = buckets[:len(dcb.counts.buckets)]
	copy(buckets, dcb.counts.buckets)
	*state = SharedState{
		State:      dcb.state,
		Generation: dcb.generation,
		Age:        dcb.counts.age,
		Counts:     dcb.counts.Counts,
		Buckets:    buckets,
		Start:      dcb.start,
		Expiry:     dcb.expiry,
	}
}

func (dcb *DistributedCircuitBreaker[T]) State() (state State, err error) {
	err = dcb.lock()
	if err != nil {
		return state, err
	}
	defer func() {
		e := dcb.unlock()
		if err == nil {
			err = e
		}
	}()

	shared := sharedStatePool.Get().(*SharedState)
	defer releaseSharedState(shared)

	err = dcb.readSharedState(shared)
	if err != nil {
		return shared.State, err
	}

	dcb.inject(*shared)
	state = dcb.CircuitBreaker.State()
	dcb.extractInto(shared)

	err = dcb.writeSharedState(shared)
	return state, err
}

func (dcb *DistributedCircuitBreaker[T]) Execute(req func() (T, error)) (t T, err error) {
	err = dcb.lock()
	if err != nil {
		return t, err
	}
	defer func() {
		e := dcb.unlock()
		if err == nil {
			err = e
		}
	}()

	shared := sharedStatePool.Get().(*SharedState)
	defer releaseSharedState(shared)

	err = dcb.readSharedState(shared)
	if err != nil {
		return t, err
	}

	dcb.inject(*shared)
	t, err = dcb.CircuitBreaker.Execute(req)
	dcb.extractInto(shared)

	e := dcb.writeSharedState(shared)
	if e != nil {
		return t, e
	}

	return t, err
}
