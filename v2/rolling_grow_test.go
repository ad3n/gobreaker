package gobreaker

import (
	"math/rand/v2"
	"reflect"
	"strconv"
	"testing"
)

func growReference(rc *rollingCounts, age uint64) {
	if age <= rc.age {
		return
	}

	diff := age - rc.age
	if diff >= uint64(len(rc.buckets)) {
		rc.clear()
		rc.age = age
		return
	}

	for range diff {
		rc.age++
		index := rc.current()
		subtractReference(rc, index)
		rc.buckets[index].clear()
	}
}

func TestRollingGrowMatchesReference(t *testing.T) {
	rng := rand.New(rand.NewPCG(76, 19))
	counts := func() Counts {
		return Counts{rng.Uint32(), rng.Uint32(), rng.Uint32(), rng.Uint32(), rng.Uint32(), rng.Uint32()}
	}
	for range 10000 {
		actual := newRollingCounts(rng.Int64N(100))
		actual.age = []uint64{0, ^uint64(0) - 3, rng.Uint64()}[rng.IntN(3)]
		actual.Counts = counts()
		for i := range actual.buckets {
			if rng.IntN(3) == 0 {
				actual.buckets[i] = counts()
			}
		}

		expected := *actual
		expected.buckets = make([]Counts, len(actual.buckets))
		copy(expected.buckets, actual.buckets)
		target := actual.age + rng.Uint64N(110)
		actual.grow(target)
		growReference(&expected, target)
		if !reflect.DeepEqual(*actual, expected) {
			t.Fatalf("growth to %d differs: got %+v, want %+v", target, *actual, expected)
		}
	}
}

func BenchmarkRollingGrow(b *testing.B) {
	for _, size := range []int64{60, 6000} {
		for _, filled := range []bool{false, true} {
			b.Run(strconv.FormatInt(size, 10)+"/filled="+strconv.FormatBool(filled), func(b *testing.B) {
				for _, optimized := range []bool{false, true} {
					b.Run("optimized="+strconv.FormatBool(optimized), func(b *testing.B) {
						rc := newRollingCounts(size)
						initial := make([]Counts, size)
						if filled {
							for i := range initial {
								initial[i] = Counts{Requests: 1, TotalSuccesses: 1, ConsecutiveSuccesses: 1}
							}
						}

						b.ReportAllocs()
						b.ResetTimer()
						for range b.N {
							copy(rc.buckets, initial)
							rc.age = 0
							if optimized {
								rc.grow(10)
								continue
							}

							for range 10 {
								rc.roll()
							}
						}
					})
				}
			})
		}
	}
}
