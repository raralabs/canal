package pipeline

import "sync"

// metadata is a data holder that is used to store information generated during execution.
type metadata struct {
	dataMu *sync.Mutex
	data   map[string]interface{}
}

// newMetadata creates a new metadata and returns it.
func newMetadata() *metadata {
	return &metadata{
		dataMu: &sync.Mutex{},
		data: make(map[string]interface{}),
	}
}

// add adds a key, value pair to the metadata.
func (m *metadata) add(key string, value interface{}) {
	m.dataMu.Lock()
	defer m.dataMu.Unlock()

	m.data[key] = value
}

// dataCopy returns a copy of the data held by the metadata.
func (m *metadata) dataCopy() map[string]interface{} {
	d := make(map[string]interface{})

	m.dataMu.Lock()
	for k,v := range m.data {
		d[k] = v
	}
	m.dataMu.Unlock()

	return d
}
