package faker

import (
	"math/rand"
	"time"
)

type Stream struct {
	m map[string][]interface{}
}

func NewFake(m map[string][]interface{}) *Stream {

	rand.Seed(time.Now().Unix())
	return &Stream{
		m: m,
	}
}

func (f *Stream) Random(record map[string]interface{}) {

	for k, v := range f.m {
		if v == nil {
			record[k] = nil
			continue
		}
		choice := v[rand.Intn(len(v))]
		record[k] = choice
	}
}
