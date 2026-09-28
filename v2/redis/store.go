package redis

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/ad3n/gobreaker/v2"
	"github.com/go-redsync/redsync/v4"
	"github.com/go-redsync/redsync/v4/redis/goredis/v9"
	"github.com/redis/go-redis/v9"
)

type Store struct {
	ctx    context.Context
	client redis.UniversalClient
	rs     *redsync.Redsync
	mutex  map[string]*storeLock
	mu     sync.Mutex
}

type storeLock struct {
	mutex        *redsync.Mutex
	done         chan struct{}
	unlocking    bool
	ready        bool
	unlockFailed bool
}

func NewStore(addr string) gobreaker.SharedDataStore {
	client := redis.NewClient(&redis.Options{
		Addr: addr,
	})
	return &Store{
		ctx:    context.Background(),
		client: client,
		rs:     redsync.New(goredis.NewPool(client)),
		mutex:  map[string]*storeLock{},
	}
}

func NewStoreFromClient(client redis.UniversalClient) gobreaker.SharedDataStore {
	return &Store{
		ctx:    context.Background(),
		client: client,
		rs:     redsync.New(goredis.NewPool(client)),
		mutex:  map[string]*storeLock{},
	}
}

func (rs *Store) Lock(name string) error {
	var timer *time.Timer
	defer func() {
		if timer != nil {
			timer.Stop()
		}
	}()

	for {
		rs.mu.Lock()
		owner := rs.mutex[name]
		if owner == nil {
			owner = &storeLock{
				mutex: rs.rs.NewMutex(name, redsync.WithExpiry(5*time.Second)),
				done:  make(chan struct{}),
			}
			rs.mutex[name] = owner
			rs.mu.Unlock()
			if err := owner.mutex.Lock(); err != nil {
				rs.mu.Lock()
				delete(rs.mutex, name)
				close(owner.done)
				rs.mu.Unlock()
				return err
			}

			rs.mu.Lock()
			owner.ready = true
			rs.mu.Unlock()

			return nil
		}

		if owner.unlockFailed && !owner.unlocking {
			owner.unlocking = true
			rs.mu.Unlock()
			released, err := rs.unlock(name, owner)
			if !released {
				return err
			}

			continue
		}

		done := owner.done
		rs.mu.Unlock()
		if timer == nil {
			timer = time.NewTimer(5 * time.Second)
		}

		select {
		case <-done:
		case <-timer.C:
			return redsync.ErrFailed
		}
	}
}

func (rs *Store) Unlock(name string) error {
	rs.mu.Lock()
	owner := rs.mutex[name]
	if owner == nil || !owner.ready || owner.unlocking {
		rs.mu.Unlock()
		return errors.New("unlock failed")
	}

	owner.unlocking = true
	rs.mu.Unlock()
	_, err := rs.unlock(name, owner)
	return err
}

func (rs *Store) unlock(name string, owner *storeLock) (bool, error) {
	ok, err := owner.mutex.Unlock()
	var taken *redsync.ErrTaken
	released := ok || err == nil || errors.Is(err, redsync.ErrLockAlreadyExpired) || errors.As(err, &taken)
	rs.mu.Lock()
	if released {
		delete(rs.mutex, name)
		close(owner.done)
	}

	owner.unlocking = false
	owner.unlockFailed = !released
	rs.mu.Unlock()
	if !ok || err != nil {
		return released, errors.New("unlock failed")
	}

	return released, nil
}

func (rs *Store) GetData(name string) ([]byte, error) {
	return rs.client.Get(rs.ctx, name).Bytes()
}

func (rs *Store) SetData(name string, data []byte) error {
	return rs.client.Set(rs.ctx, name, data, 0).Err()
}

func (rs *Store) Close() {
	rs.client.Close()
}
