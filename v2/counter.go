package gobreaker

type Counts struct {
	Requests             uint32
	TotalSuccesses       uint32
	TotalFailures        uint32
	TotalExclusions      uint32
	ConsecutiveSuccesses uint32
	ConsecutiveFailures  uint32
}

func (c *Counts) onRequest() {
	c.Requests++
}

func (c *Counts) onSuccess() {
	c.TotalSuccesses++
	c.ConsecutiveSuccesses++
	c.ConsecutiveFailures = 0
}

func (c *Counts) onFailure() {
	c.TotalFailures++
	c.ConsecutiveFailures++
	c.ConsecutiveSuccesses = 0
}

func (c *Counts) onExclusion() {
	c.TotalExclusions++
}

func (c *Counts) validRequests() uint32 {
	if c.Requests < c.TotalExclusions {
		return 0
	}

	return c.Requests - c.TotalExclusions
}

func (c *Counts) clear() {
	*c = Counts{}
}

type rollingCounts struct {
	Counts

	age     uint64
	buckets []Counts
}

func newRollingCounts(numBuckets int64) *rollingCounts {
	if numBuckets < 0 {
		numBuckets = 0
	}

	return &rollingCounts{
		buckets: make([]Counts, numBuckets),
	}
}

func (rc *rollingCounts) index(age uint64) uint64 {
	if len(rc.buckets) == 0 {
		return 0
	}

	return age % uint64(len(rc.buckets))
}

func (rc *rollingCounts) current() uint64 {
	return rc.index(rc.age)
}

func (rc *rollingCounts) onRequest() {
	rc.Counts.onRequest()
	rc.buckets[rc.current()].onRequest()
}

func (rc *rollingCounts) onSuccess(age uint64) {
	if age > rc.age {
		return
	}

	if rc.age-age < uint64(len(rc.buckets)) {
		rc.Counts.onSuccess()
		rc.buckets[rc.index(age)].onSuccess()
	}
}

func (rc *rollingCounts) onFailure(age uint64) {
	if age > rc.age {
		return
	}

	if rc.age-age < uint64(len(rc.buckets)) {
		rc.Counts.onFailure()
		rc.buckets[rc.index(age)].onFailure()
	}
}

func (rc *rollingCounts) onExclusion(age uint64) {
	if age > rc.age {
		return
	}

	if rc.age-age < uint64(len(rc.buckets)) {
		rc.Counts.onExclusion()
		rc.buckets[rc.index(age)].onExclusion()
	}
}

func (rc *rollingCounts) clear() {
	rc.Counts.clear()

	rc.age = 0

	clear(rc.buckets)
}

func (rc *rollingCounts) roll() {
	rc.age++
	if len(rc.buckets) == 0 {
		return
	}

	current := rc.current()
	rc.subtract(current)
	rc.buckets[current].clear()
}

func (rc *rollingCounts) subtract(oldest uint64) {
	length := uint64(len(rc.buckets))
	if length == 0 {
		return
	}

	oldest = oldest % length
	bucket := rc.buckets[oldest]

	totalSuccesses := bucket.ConsecutiveSuccesses
	totalFailures := bucket.ConsecutiveFailures
	for _, other := range rc.buckets[:oldest] {
		totalSuccesses += other.TotalSuccesses
		totalFailures += other.TotalFailures
	}

	for _, other := range rc.buckets[oldest+1:] {
		totalSuccesses += other.TotalSuccesses
		totalFailures += other.TotalFailures
	}

	if rc.ConsecutiveSuccesses == totalSuccesses {
		rc.ConsecutiveSuccesses -= min(rc.ConsecutiveSuccesses, bucket.ConsecutiveSuccesses)
	}

	if rc.ConsecutiveFailures == totalFailures {
		rc.ConsecutiveFailures -= min(rc.ConsecutiveFailures, bucket.ConsecutiveFailures)
	}

	rc.Requests -= min(rc.Requests, bucket.Requests)
	rc.TotalSuccesses -= min(rc.TotalSuccesses, bucket.TotalSuccesses)
	rc.TotalFailures -= min(rc.TotalFailures, bucket.TotalFailures)
	rc.TotalExclusions -= min(rc.TotalExclusions, bucket.TotalExclusions)
}

func (rc *rollingCounts) grow(age uint64) {
	if age <= rc.age {
		return
	}

	diff := age - rc.age
	if diff >= uint64(len(rc.buckets)) {
		rc.clear()
		rc.age = age
		return
	}

	if diff == 1 {
		rc.roll()
		return
	}

	for range diff {
		rc.age++
		current := rc.current()
		if rc.buckets[current] == (Counts{}) {
			continue
		}

		rc.subtract(current)
		rc.buckets[current].clear()
	}
}

func (rc *rollingCounts) bucketAt(index int) Counts {
	bucketLen := len(rc.buckets)
	if bucketLen == 0 {
		return Counts{}
	}

	idx := (index%bucketLen + bucketLen) % bucketLen
	if idx < 0 {
		return Counts{}
	}

	bucketIndex := (rc.current() + uint64(idx)) % uint64(bucketLen)
	return rc.buckets[bucketIndex]
}
