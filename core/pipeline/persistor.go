package pipeline

import "time"

// IPersistor defines the interface for:
// 1. Saving Keys and Values with TTL.
// 2. Retrieving value of any Key.
// 3. Ranging on the Key prefix to get the list of keys and values. Supports paginating the data as well.
type IPersistor interface {
	// Stores a key, value pair with default TTL.
	Store(key string, value interface{})

	// Stores a key, value pair with given TTL.
	StoreWithTTL(key string, value interface{}, ttl time.Duration)

	// Provides TTL for a key. Returns nil error on success.
	Expire(key string, ttl time.Duration) error

	// Returns the value associated with the key. Error is nil on success.
	Get(key string) (interface{}, error)

	// Returns the length of the pairs stored
	Length() int

	// Returns the list of keys, with provided length.
	// If numItems is larger than available keys, all available keys are returned.
	// If numItems is negative all the available keys are returned.
	Iterate(numItems int) []string

	// Resets the paging. Iterate returns the keys from beginning after this call.
	ResetPaging()
}
