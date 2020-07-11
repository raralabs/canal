package maths

import (
	"math"
	"math/rand"
)

type ReservoirSampling struct {
	k uint64 // The number of elements to be picked
}

func NewReservoirSampling(k uint64) *ReservoirSampling {
	return &ReservoirSampling{
		k: k,
	}
}

// Sample returns (index, switched).
// If switched is false, the reservoir must not be changed.
func (rs *ReservoirSampling) Sample(count uint64) (uint64, bool) {
	if count < rs.k {
		return count, true
	}

	rn := rand.Float64()
	index := uint64(math.Floor(float64(count)*rn))
	if index < rs.k {
		return index, true
	}

	return 0, false
}
