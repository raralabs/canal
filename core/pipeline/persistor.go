package pipeline

import (
	"time"
)

// IPersistor defines the interface for:
// 1. Saving Keys and Values with TTL.
// 2. Retrieving value of any Key.
// 3. Ranging on the Key prefix to get the list of keys and values. Supports paginating the data as well.
type IPersistor interface {
	// Stores a key, value pair with default TTL.
	Store(key string, value []byte) error

	// Stores a map with default TTL.
	StoreMap(m map[string][]byte) error

	// Stores a key, value pair with given TTL.
	StoreWithTTL(key string, value []byte, ttl time.Duration) error

	// Stores a map with given TTL.
	StoreMapWithTTL(m map[string][]byte, ttl time.Duration) error

	// Returns the value associated with the key. Error is nil on success.
	Get(key string) ([]byte, error)

	// Returns the list of pairs, with provided length.
	// If numItems is larger than available pairs, all available pairs are returned.
	Iterate(numItems int) map[string][]byte

	// Resets the paging. Iterate returns the pairs from beginning after this call.
	ResetPaging()

	// Closes the Persistor
	Close()
}
