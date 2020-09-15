package pipeline

import (
	"github.com/stretchr/testify/assert"
	"testing"

)

func TestMetaData_newMetaData(t *testing.T){
	t.Run("newMetaData", func(t *testing.T) {
		metaData := newMetadata()
	})

}

func TestMetaData_messageRate(t *testing.T){
	t.Run("newMetaData", func(t *testing.T) {
		metaData := newMessageRate(10,uint64(4))
		assert.Equal(t,10,metaData.duration,"duration mismatch")
		assert.Equal(t,uint64(4),metaData.bufSize,"")
	})
}
