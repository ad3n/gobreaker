package redis

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestConcurrentLockSameKey(t *testing.T) {
	mr, store := setupTestRedis()
	defer mr.Close()

	defer store.Close()

	var wg sync.WaitGroup
	var active atomic.Int32
	var completed atomic.Int32
	start := make(chan struct{})
	for range 8 {
		wg.Go(func() {

			<-start
			for range 10 {
				if err := store.Lock("shared"); err != nil {
					t.Error(err)
					return
				}

				if active.Add(1) != 1 {
					t.Error("overlapping lock ownership")
				}

				time.Sleep(time.Millisecond)
				active.Add(-1)
				if err := store.Unlock("shared"); err != nil {
					t.Error(err)
					return
				}

				completed.Add(1)
			}
		})
	}

	close(start)
	wg.Wait()
	if completed.Load() != 80 {
		t.Fatalf("only %d critical sections completed", completed.Load())
	}

	if len(store.mutex) != 0 {
		t.Fatal("mutex entries leaked")
	}
}

func TestFailedLockPreservesOwner(t *testing.T) {
	mr, store := setupTestRedis()
	defer mr.Close()

	defer store.Close()

	if err := store.Lock("shared"); err != nil {
		t.Fatal(err)
	}

	owner := store.mutex["shared"]
	if err := store.Lock("shared"); err == nil {
		t.Fatal("acquired a lock already held")
	}

	if store.mutex["shared"] != owner {
		t.Fatal("failed acquisition replaced the owner")
	}

	if err := store.Unlock("shared"); err != nil {
		t.Fatal(err)
	}

	if err := mr.Set("external", "another owner"); err != nil {
		t.Fatal(err)
	}

	if err := store.Lock("external"); err == nil {
		t.Fatal("acquired externally held lock")
	}

	if len(store.mutex) != 0 {
		t.Fatal("failed acquisition leaked an entry")
	}
}

func TestExpiredLeasePreservesLocalOwner(t *testing.T) {
	mr, store := setupTestRedis()
	defer mr.Close()

	defer store.Close()

	if err := store.Lock("shared"); err != nil {
		t.Fatal(err)
	}

	owner := store.mutex["shared"]
	mr.FastForward(6 * time.Second)
	if err := store.Lock("shared"); err == nil {
		t.Fatal("replaced an owner that has not unlocked")
	}

	if store.mutex["shared"] != owner {
		t.Fatal("local owner changed after lease expiry")
	}

	if err := store.Unlock("shared"); err == nil {
		t.Fatal("expired lease unexpectedly unlocked")
	}

	if err := store.Lock("shared"); err != nil {
		t.Fatal(err)
	}

	if err := store.Unlock("shared"); err != nil {
		t.Fatal(err)
	}

	if len(store.mutex) != 0 {
		t.Fatal("local ownership leaked")
	}
}

func TestExpiredUnlockPreservesExternalOwner(t *testing.T) {
	mr, store := setupTestRedis()
	defer mr.Close()

	defer store.Close()

	if err := store.Lock("shared"); err != nil {
		t.Fatal(err)
	}

	mr.FastForward(6 * time.Second)
	if err := mr.Set("shared", "new-owner"); err != nil {
		t.Fatal(err)
	}

	if err := store.Unlock("shared"); err == nil {
		t.Fatal("unlocked a lease owned by another store")
	}

	value, err := mr.Get("shared")
	if err != nil || value != "new-owner" {
		t.Fatalf("external lease changed: %q, %v", value, err)
	}

	if len(store.mutex) != 0 {
		t.Fatal("lost ownership leaked")
	}
}

func TestUnlockRetryPreservesOwner(t *testing.T) {
	mr, store := setupTestRedis()
	defer mr.Close()

	defer store.Close()

	if err := store.Lock("shared"); err != nil {
		t.Fatal(err)
	}

	owner := store.mutex["shared"]
	mr.SetError("temporary failure")
	if err := store.Unlock("shared"); err == nil {
		t.Fatal("expected Redis failure")
	}

	if store.mutex["shared"] != owner {
		t.Fatal("transient failure discarded ownership")
	}

	mr.SetError("")
	if err := store.Unlock("shared"); err != nil {
		t.Fatal(err)
	}

	if len(store.mutex) != 0 {
		t.Fatal("ownership leaked after retry")
	}
}

func TestLockRecoversAfterUnlockFailure(t *testing.T) {
	mr, store := setupTestRedis()
	defer mr.Close()

	defer store.Close()

	if err := store.Lock("shared"); err != nil {
		t.Fatal(err)
	}

	mr.SetError("temporary failure")
	if err := store.Unlock("shared"); err == nil {
		t.Fatal("expected Redis failure")
	}

	mr.SetError("")
	if err := store.Lock("shared"); err != nil {
		t.Fatal(err)
	}

	if err := store.Unlock("shared"); err != nil {
		t.Fatal(err)
	}

	if len(store.mutex) != 0 {
		t.Fatal("ownership leaked during recovery")
	}
}
