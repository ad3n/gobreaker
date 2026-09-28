package gobreaker

import (
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"
	"runtime"
	"sync"
	"testing"
	"time"
)

func TestSharedStateReset(t *testing.T) {
	for _, capacity := range []int{0, 1, 60, maxPooledBuckets, maxPooledBuckets + 1} {
		t.Run(fmt.Sprint(capacity), func(t *testing.T) {
			buckets := make([]Counts, capacity)
			for i := range buckets {
				buckets[i] = Counts{1, 2, 3, 4, 5, 6}
			}

			state := SharedState{
				State: StateOpen, Generation: 7, Age: 8,
				Counts: Counts{1, 2, 3, 4, 5, 6}, Buckets: buckets[:0],
				Start: time.Now(), Expiry: time.Now().Add(time.Hour),
			}
			resetSharedState(&state)
			if state.State != StateClosed || state.Generation != 0 || state.Age != 0 || state.Counts != (Counts{}) || !state.Start.IsZero() || !state.Expiry.IsZero() {
				t.Fatalf("state retained values: %+v", state)
			}

			if len(state.Buckets) != 0 || cap(state.Buckets) > maxPooledBuckets {
				t.Fatalf("unbounded or nonempty buffer: len=%d cap=%d", len(state.Buckets), cap(state.Buckets))
			}

			if capacity > maxPooledBuckets {
				if state.Buckets != nil {
					t.Fatal("oversized buffer retained")
				}

				return
			}

			if cap(state.Buckets) != capacity {
				t.Fatal("reusable capacity discarded")
			}

			for _, bucket := range state.Buckets[:cap(state.Buckets)] {
				if bucket != (Counts{}) {
					t.Fatal("unused capacity retained old counters")
				}
			}
		})
	}
}

func TestSharedStateReuseMatchesFreshDecode(t *testing.T) {
	store := NewMockStore()
	cb, err := NewDistributedCircuitBreaker[int](store, Settings{Name: "decode"})
	if err != nil {
		t.Fatal(err)
	}

	payloads := []string{
		`{"state":2,"generation":42,"age":3,"counts":{"Requests":9},"buckets":[{"Requests":9,"TotalFailures":4},{"TotalSuccesses":8}],"start":"2026-01-01T01:00:00Z","expiry":"2026-01-02T01:00:00Z"}`,
		`{"buckets":[{},null,{"TotalExclusions":2}]}`,
		`{}`,
		`null`,
		`{"buckets":null,"start":null,"expiry":null}`,
		`{"buckets":[]}`,
		`{"buckets":[{"Requests":1}],"buckets":[{"TotalSuccesses":2}]}`,
		`{"state":2,"generation":"invalid","buckets":[{}]}`,
		`{"state":2,"buckets":[`,
	}
	state := new(SharedState)
	for range 5 {
		for _, payload := range payloads {
			resetSharedState(state)
			if err := store.SetData(cb.sharedStateKey(), []byte(payload)); err != nil {
				t.Fatal(err)
			}

			var fresh SharedState
			wantErr := json.Unmarshal([]byte(payload), &fresh)
			gotErr := cb.readSharedState(state)
			if fmt.Sprint(gotErr) != fmt.Sprint(wantErr) {
				t.Fatalf("payload %s: got error %v, want %v", payload, gotErr, wantErr)
			}

			got := *state
			if len(got.Buckets) == 0 {
				got.Buckets = nil
			}

			if len(fresh.Buckets) == 0 {
				fresh.Buckets = nil
			}

			if !reflect.DeepEqual(fresh, got) {
				t.Fatalf("payload %s: reused=%+v fresh=%+v", payload, got, fresh)
			}
		}
	}
}

type retainingStore struct {
	*MockStore
	retained [][]byte
	expected [][]byte
}

func (s *retainingStore) SetData(name string, data []byte) error {
	s.retained = append(s.retained, data)
	s.expected = append(s.expected, bytes.Clone(data))
	return s.MockStore.SetData(name, data)
}

func TestSharedStatePoolRetainedStoreData(t *testing.T) {
	store := &retainingStore{MockStore: NewMockStore()}
	cb, err := NewDistributedCircuitBreaker[int](store, Settings{Name: "retained", Interval: time.Hour, BucketPeriod: time.Minute})
	if err != nil {
		t.Fatal(err)
	}

	for range 100 {
		if _, err := cb.Execute(func() (int, error) { return 1, nil }); err != nil {
			t.Fatal(err)
		}

		if _, err := cb.State(); err != nil {
			t.Fatal(err)
		}
	}

	for i := range store.retained {
		if !bytes.Equal(store.retained[i], store.expected[i]) {
			t.Fatalf("store-owned bytes mutated after SetData at index %d", i)
		}
	}
}

func TestSharedStatePoolAcrossBreakersAndGC(t *testing.T) {
	var wg sync.WaitGroup
	for worker := range 12 {
		wg.Go(func() {
			cb, err := NewDistributedCircuitBreaker[int](NewMockStore(), Settings{Name: fmt.Sprint(worker), Interval: 60 * time.Hour, BucketPeriod: time.Hour})
			if err != nil {
				t.Error(err)
				return
			}

			for i := range 30 {
				if i%10 == 0 {
					runtime.GC()
				}

				if _, err := cb.Execute(func() (int, error) { return worker, nil }); err != nil {
					t.Error(err)
					return
				}

				state, err := cb.State()
				if err != nil || state != StateClosed {
					t.Errorf("state=%v err=%v", state, err)
					return
				}
			}

			snapshot := cb.extract()
			if snapshot.Counts.Requests != 30 || snapshot.Counts.TotalSuccesses != 30 {
				t.Errorf("cross-breaker contamination: %+v", snapshot.Counts)
			}
		})
	}

	wg.Wait()
}

func BenchmarkDistributedBuckets(b *testing.B) {
	for _, buckets := range []int{1, 60, 1024, 2048} {
		b.Run(fmt.Sprint(buckets), func(b *testing.B) {
			cb, err := NewDistributedCircuitBreaker[int](NewMockStore(), Settings{Name: "pooled", Interval: time.Duration(buckets) * time.Hour, BucketPeriod: time.Hour})
			if err != nil {
				b.Fatal(err)
			}

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := cb.Execute(func() (int, error) { return 1, nil }); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
