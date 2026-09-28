package gobreaker

import (
	"bytes"
	"encoding/json"
	"math/rand/v2"
	"reflect"
	"strconv"
	"testing"
	"time"
)

func TestSharedStateEncodingMatchesJSON(t *testing.T) {
	rng := rand.New(rand.NewPCG(42, 17))
	times := []time.Time{
		{}, time.Now(), time.Unix(-1, 999999999),
		time.Date(9999, 12, 31, 23, 59, 59, 123456789, time.UTC),
		time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC),
		time.Date(-1, 1, 1, 0, 0, 0, 0, time.UTC),
	}
	for _, offset := range []int{-86400, -86399, -1, 1, 86399, 86400} {
		times = append(times, time.Unix(0, 123).In(time.FixedZone("custom", offset)))
	}

	check := func(state *SharedState) {
		t.Helper()
		want, wantErr := json.Marshal(state)
		got, gotErr := marshalSharedState(state)
		if !bytes.Equal(got, want) || reflect.TypeOf(gotErr) != reflect.TypeOf(wantErr) {
			t.Fatalf("encoding mismatch: got %s, %v; want %s, %v", got, gotErr, want, wantErr)
		}

		if state != nil && gotErr == nil && cap(got) != len(got) {
			t.Fatalf("incorrect encoding size: capacity %d, length %d", cap(got), len(got))
		}

		if wantErr != nil && gotErr.Error() != wantErr.Error() {
			t.Fatalf("error mismatch: got %v; want %v", gotErr, wantErr)
		}
	}
	check(nil)
	check(&SharedState{})
	check(&SharedState{Buckets: []Counts{}})
	maximum := Counts{^uint32(0), ^uint32(0), ^uint32(0), ^uint32(0), ^uint32(0), ^uint32(0)}
	maxState := State(int(^uint(0) >> 1))
	for _, value := range []State{maxState, -maxState - 1} {
		check(&SharedState{State: value, Generation: ^uint64(0), Age: ^uint64(0), Counts: maximum, Buckets: []Counts{maximum}})
	}

	for range 1000 {
		state := SharedState{
			State: State(rng.Int64()), Generation: rng.Uint64(), Age: rng.Uint64(),
			Start: times[rng.IntN(len(times))], Expiry: times[rng.IntN(len(times))],
			Buckets: make([]Counts, rng.IntN(30)),
		}
		for i := range state.Buckets {
			state.Buckets[i] = Counts{rng.Uint32(), rng.Uint32(), rng.Uint32(), rng.Uint32(), rng.Uint32(), rng.Uint32()}
		}

		if len(state.Buckets) > 0 {
			state.Counts = state.Buckets[0]
		}

		check(&state)
	}
}

func BenchmarkSharedStateEncoding(b *testing.B) {
	for _, size := range []int{1, 60, 1024} {
		state := SharedState{Buckets: make([]Counts, size), Start: time.Now()}
		b.Run(strconv.Itoa(size), func(b *testing.B) {
			b.Run("standard", func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					if _, err := json.Marshal(&state); err != nil {
						b.Fatal(err)
					}
				}
			})
			b.Run("optimized", func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					if _, err := marshalSharedState(&state); err != nil {
						b.Fatal(err)
					}
				}
			})
		})
	}
}
