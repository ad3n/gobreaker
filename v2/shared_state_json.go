package gobreaker

import (
	"encoding/json"
	"strconv"
)

func marshalSharedState(state *SharedState) ([]byte, error) {
	if state == nil {
		return json.Marshal(state)
	}

	var startBuffer, expiryBuffer [35]byte
	start, err := state.Start.AppendText(startBuffer[:0])
	if err != nil {
		return json.Marshal(state)
	}

	expiry, err := state.Expiry.AppendText(expiryBuffer[:0])
	if err != nil {
		return json.Marshal(state)
	}

	var stateBuffer [20]byte
	stateText := strconv.AppendInt(stateBuffer[:0], int64(state.State), 10)

	size := 77 + len(stateText) + uintDigits(state.Generation) + uintDigits(state.Age) + countsJSONSize(state.Counts) + len(start) + len(expiry)
	if state.Buckets == nil {
		size += 2
	}

	for i, bucket := range state.Buckets {
		size += countsJSONSize(bucket)
		if i != 0 {
			size++
		}
	}

	data := make([]byte, 0, size)
	data = append(data, `{"state":`...)
	data = append(data, stateText...)
	data = append(data, `,"generation":`...)
	data = strconv.AppendUint(data, state.Generation, 10)
	data = append(data, `,"age":`...)
	data = strconv.AppendUint(data, state.Age, 10)
	data = append(data, `,"counts":`...)
	data = appendCountsJSON(data, state.Counts)
	data = append(data, `,"buckets":`...)
	if state.Buckets == nil {
		data = append(data, "null"...)
	}

	if state.Buckets != nil {
		data = append(data, '[')
		for i, bucket := range state.Buckets {
			if i != 0 {
				data = append(data, ',')
			}

			data = appendCountsJSON(data, bucket)
		}

		data = append(data, ']')
	}

	data = append(data, `,"start":"`...)
	data = append(data, start...)
	data = append(data, `","expiry":"`...)
	data = append(data, expiry...)

	return append(data, '"', '}'), nil
}

func appendCountsJSON(data []byte, counts Counts) []byte {
	data = append(data, `{"Requests":`...)
	data = strconv.AppendUint(data, uint64(counts.Requests), 10)
	data = append(data, `,"TotalSuccesses":`...)
	data = strconv.AppendUint(data, uint64(counts.TotalSuccesses), 10)
	data = append(data, `,"TotalFailures":`...)
	data = strconv.AppendUint(data, uint64(counts.TotalFailures), 10)
	data = append(data, `,"TotalExclusions":`...)
	data = strconv.AppendUint(data, uint64(counts.TotalExclusions), 10)
	data = append(data, `,"ConsecutiveSuccesses":`...)
	data = strconv.AppendUint(data, uint64(counts.ConsecutiveSuccesses), 10)
	data = append(data, `,"ConsecutiveFailures":`...)
	data = strconv.AppendUint(data, uint64(counts.ConsecutiveFailures), 10)
	return append(data, '}')
}

func uintDigits(value uint64) int {
	digits := 1
	for value >= 10 {
		value /= 10
		digits++
	}

	return digits
}

func countsJSONSize(counts Counts) int {
	return 114 + uintDigits(uint64(counts.Requests)) + uintDigits(uint64(counts.TotalSuccesses)) + uintDigits(uint64(counts.TotalFailures)) + uintDigits(uint64(counts.TotalExclusions)) + uintDigits(uint64(counts.ConsecutiveSuccesses)) + uintDigits(uint64(counts.ConsecutiveFailures))
}
