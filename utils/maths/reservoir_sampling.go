package maths

import (
	"math"
	"math/rand"
)

// ReservoirSample returns (index, switched).
// If switched is false, the reservoir must not be changed.
func ReservoirSample(k, count uint64) (uint64, bool) {
	if count < k {
		return count, true
	}

	rn := rand.Float64()
	index := uint64(math.Floor(float64(count)*rn))
	if index < k {
		return index, true
	}

	return 0, false
}
