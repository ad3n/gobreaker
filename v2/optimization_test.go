package gobreaker

import (
	"errors"
	"fmt"
	"math/rand"
	"reflect"
	"sync"
	"testing"
	"time"
)

type transactionStore struct {
	*MockStore
	transaction sync.Mutex
	beforeLock  func()
	getErr      error
	setErr      error
	unlockErr   error
	unlocks     int
}

func (s *transactionStore) Lock(string) error {
	s.transaction.Lock()
	if s.beforeLock != nil {
		s.beforeLock()
		s.beforeLock = nil
	}

	return nil
}

func (s *transactionStore) Unlock(string) error {
	s.unlocks++
	s.transaction.Unlock()
	return s.unlockErr
}

func (s *transactionStore) GetData(name string) ([]byte, error) {
	if s.getErr != nil {
		return []byte("{}"), s.getErr
	}

	return s.MockStore.GetData(name)
}

func (s *transactionStore) SetData(name string, data []byte) error {
	if s.setErr != nil {
		return s.setErr
	}

	return s.MockStore.SetData(name, data)
}

func TestDistributedReadsAfterLock(t *testing.T) {
	for _, operation := range []string{"execute", "state"} {
		t.Run(operation, func(t *testing.T) {
			store := &transactionStore{MockStore: NewMockStore()}
			cb, err := NewDistributedCircuitBreaker[int](store, Settings{Name: "shared"})
			if err != nil {
				t.Fatal(err)
			}

			store.beforeLock = func() {
				state := cb.extract()
				state.Counts = Counts{Requests: 1, TotalSuccesses: 1, ConsecutiveSuccesses: 1}
				state.Buckets[0] = state.Counts
				if err := cb.setSharedState(state); err != nil {
					t.Fatal(err)
				}
			}
			want := uint32(1)
			switch operation {
			case "execute":
				_, err = cb.Execute(func() (int, error) { return 42, nil })
				want++
			case "state":
				_, err = cb.State()
			}

			if err != nil {
				t.Fatal(err)
			}

			shared, err := cb.getSharedState()
			if err != nil {
				t.Fatal(err)
			}

			if shared.Counts.Requests != want || shared.Counts.TotalSuccesses != want {
				t.Fatalf("lost update: got %+v, want %d requests and successes", shared.Counts, want)
			}
		})
	}
}

func TestDistributedConcurrentTransactions(t *testing.T) {
	store := &transactionStore{MockStore: NewMockStore()}
	breakers := make([]*DistributedCircuitBreaker[int], 2)
	for i := range breakers {
		cb, err := NewDistributedCircuitBreaker[int](store, Settings{Name: "shared"})
		if err != nil {
			t.Fatal(err)
		}

		breakers[i] = cb
	}

	var wg sync.WaitGroup
	for worker := range 8 {
		cb := breakers[worker%len(breakers)]
		wg.Go(func() {
			for range 50 {
				if _, err := cb.Execute(func() (int, error) { return 42, nil }); err != nil {
					t.Error(err)
				}

				if _, err := cb.State(); err != nil {
					t.Error(err)
				}
			}
		})
	}

	wg.Wait()
	shared, err := breakers[0].getSharedState()
	if err != nil {
		t.Fatal(err)
	}

	if shared.Counts.Requests != 400 || shared.Counts.TotalSuccesses != 400 {
		t.Fatalf("lost concurrent requests: %+v", shared.Counts)
	}
}

func TestDistributedErrorCleanup(t *testing.T) {
	failure := errors.New("store failure")
	for _, operation := range []string{"constructor-get", "constructor-set", "execute-get", "state-get", "execute-set", "state-set", "execute-unlock", "state-unlock", "request-error", "panic"} {
		t.Run(operation, func(t *testing.T) {
			store := &transactionStore{MockStore: NewMockStore()}
			if operation == "constructor-get" {
				store.getErr = failure
			}

			if operation == "constructor-set" {
				store.setErr = failure
			}

			cb, err := NewDistributedCircuitBreaker[int](store, Settings{})
			if operation == "constructor-get" || operation == "constructor-set" {
				if cb != nil || err != failure || store.unlocks != 1 {
					t.Fatalf("constructor cleanup: cb=%v err=%v unlocks=%d", cb, err, store.unlocks)
				}

				return
			}

			if err != nil {
				t.Fatal(err)
			}

			store.unlockErr = errors.New("unlock failure")
			switch operation {
			case "execute-get", "state-get":
				store.getErr = failure
			case "execute-set", "state-set":
				store.setErr = failure
			}

			called := false
			request := func() (int, error) {
				called = true
				if operation == "panic" {
					panic(failure)
				}

				if operation == "request-error" {
					return 42, failure
				}

				return 42, nil
			}
			switch operation {
			case "state-get", "state-set", "state-unlock":
				_, err = cb.State()
			case "panic":
				func() {
					defer func() {
						if got := recover(); got != failure {
							t.Errorf("panic changed: %v", got)
						}
					}()

					_, _ = cb.Execute(request)
				}()
			default:
				_, err = cb.Execute(request)
			}

			if operation != "panic" {
				want := failure
				if operation == "execute-unlock" || operation == "state-unlock" {
					want = store.unlockErr
				}

				if err != want {
					t.Fatalf("error precedence: got %v, want %v", err, want)
				}
			}

			if operation == "execute-get" && called {
				t.Fatal("request ran after failed read")
			}

			if store.unlocks != 2 {
				t.Fatalf("lock was not released exactly once: %d", store.unlocks)
			}

			if !store.transaction.TryLock() {
				t.Fatal("transaction remains locked")
			}

			store.transaction.Unlock()
		})
	}
}

func TestDistributedSnapshotOwnership(t *testing.T) {
	cb, err := NewDistributedCircuitBreaker[int](NewMockStore(), Settings{Interval: time.Minute, BucketPeriod: time.Second})
	if err != nil {
		t.Fatal(err)
	}

	for _, size := range []int{60, 2, 100, 0, 1} {
		shared := SharedState{Buckets: make([]Counts, size)}
		for i := range shared.Buckets {
			shared.Buckets[i].Requests = uint32(i + 1)
		}

		cb.inject(shared)
		snapshot := cb.extract()
		if !reflect.DeepEqual(snapshot.Buckets, shared.Buckets) {
			t.Fatal("snapshot differs from injected buckets")
		}

		if size == 0 {
			continue
		}

		shared.Buckets[0].Requests = 1000
		if cb.counts.buckets[0].Requests != 1 {
			t.Fatal("inject aliased input buckets")
		}

		cb.inject(shared)
		if snapshot.Buckets[0].Requests != 1 {
			t.Fatal("subsequent inject mutated an extracted snapshot")
		}
	}
}

func TestDistributedKeysFollowEmbeddedBreaker(t *testing.T) {
	cb, err := NewDistributedCircuitBreaker[int](NewMockStore(), Settings{Name: "original"})
	if err != nil {
		t.Fatal(err)
	}

	for _, name := range []string{"original", "replacement", ""} {
		cb.CircuitBreaker = NewCircuitBreaker[int](Settings{Name: name})
		if cb.mutexKey() != "gobreaker:mutex:"+name || cb.sharedStateKey() != "gobreaker:state:"+name {
			t.Fatal("cached keys no longer match embedded breaker")
		}
	}
}

func subtractReference(rc *rollingCounts, oldest uint64) {
	length := uint64(len(rc.buckets))
	if length == 0 {
		return
	}

	oldest %= length
	bucket := rc.buckets[oldest]
	success := bucket.ConsecutiveSuccesses
	failure := bucket.ConsecutiveFailures
	for i := uint64(1); i < length; i++ {
		success += rc.buckets[(oldest+i)%length].TotalSuccesses
	}

	for i := uint64(1); i < length; i++ {
		failure += rc.buckets[(oldest+i)%length].TotalFailures
	}

	clamp := func(a, b uint32) uint32 {
		if a > b {
			return a - b
		}

		return 0
	}
	if rc.ConsecutiveSuccesses == success {
		rc.ConsecutiveSuccesses = clamp(rc.ConsecutiveSuccesses, bucket.ConsecutiveSuccesses)
	}

	if rc.ConsecutiveFailures == failure {
		rc.ConsecutiveFailures = clamp(rc.ConsecutiveFailures, bucket.ConsecutiveFailures)
	}

	rc.Requests = clamp(rc.Requests, bucket.Requests)
	rc.TotalSuccesses = clamp(rc.TotalSuccesses, bucket.TotalSuccesses)
	rc.TotalFailures = clamp(rc.TotalFailures, bucket.TotalFailures)
	rc.TotalExclusions = clamp(rc.TotalExclusions, bucket.TotalExclusions)
}

func TestRollingSubtractMatchesReference(t *testing.T) {
	random := rand.New(rand.NewSource(42))
	counts := func() Counts {
		return Counts{random.Uint32(), random.Uint32(), random.Uint32(), random.Uint32(), random.Uint32(), random.Uint32()}
	}
	for iteration := range 10000 {
		rc := newRollingCounts(int64(random.Intn(100)))
		rc.Counts = counts()
		for i := range rc.buckets {
			rc.buckets[i] = counts()
		}

		oldest := random.Uint64()
		if len(rc.buckets) != 0 && iteration%2 == 0 {
			index := oldest % uint64(len(rc.buckets))
			rc.ConsecutiveSuccesses = rc.buckets[index].ConsecutiveSuccesses
			rc.ConsecutiveFailures = rc.buckets[index].ConsecutiveFailures
			for i := uint64(1); i < uint64(len(rc.buckets)); i++ {
				bucket := rc.buckets[(index+i)%uint64(len(rc.buckets))]
				rc.ConsecutiveSuccesses += bucket.TotalSuccesses
				rc.ConsecutiveFailures += bucket.TotalFailures
			}
		}

		reference := *rc
		subtractReference(&reference, oldest)
		rc.subtract(oldest)
		if rc.Counts != reference.Counts {
			t.Fatalf("iteration %d: got %+v, want %+v", iteration, rc.Counts, reference.Counts)
		}
	}
}

func TestExecuteZeroAllocations(t *testing.T) {
	for _, mode := range []string{"success", "failure", "excluded", "open", "rolling"} {
		t.Run(mode, func(t *testing.T) {
			cb, request := performanceBreaker(mode)
			allocations := testing.AllocsPerRun(1000, func() { _, _ = cb.Execute(request) })
			if allocations != 0 {
				t.Fatalf("got %g allocations per request", allocations)
			}
		})
	}
}

func performanceBreaker(mode string) (*CircuitBreaker[int], func() (int, error)) {
	st := Settings{ReadyToTrip: func(Counts) bool { return false }}
	var result error
	if mode == "failure" || mode == "excluded" {
		result = errors.New("failure")
	}

	if mode == "excluded" {
		st.IsExcluded = func(error) bool { return true }
	}

	if mode == "rolling" {
		st.Interval = time.Minute
		st.BucketPeriod = time.Second
	}

	cb := NewCircuitBreaker[int](st)
	if mode == "open" {
		cb.setState(StateOpen, time.Now())
	}

	return cb, func() (int, error) { return 1, result }
}

func BenchmarkExecute(b *testing.B) {
	for _, mode := range []string{"success", "failure", "excluded", "open", "rolling"} {
		b.Run(mode, func(b *testing.B) {
			cb, request := performanceBreaker(mode)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, _ = cb.Execute(request)
			}
		})
	}
}

var performanceDone func(error)

func BenchmarkAllow(b *testing.B) {
	cb := NewTwoStepCircuitBreaker[int](Settings{})
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		performanceDone, _ = cb.Allow()
		performanceDone(nil)
	}
}

func BenchmarkDistributed(b *testing.B) {
	cb, err := NewDistributedCircuitBreaker[int](NewMockStore(), Settings{Name: "audit"})
	if err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := cb.Execute(func() (int, error) { return 1, nil })
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRoll(b *testing.B) {
	for _, count := range []int64{1, 60, 600, 6000} {
		b.Run(fmt.Sprint(count), func(b *testing.B) {
			rc := newRollingCounts(count)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				rc.onRequest()
				rc.onSuccess(rc.age)
				rc.roll()
			}
		})
	}
}
