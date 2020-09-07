package message

import (
	"github.com/stretchr/testify/assert"
	"reflect"
	"testing"
)
//Tests:
// - newTraceRoot
// - String()

func Test_newTraceRoot_String(t *testing.T) {
	traceEnabled := newTraceRoot(true)
	traceDisabled := newTraceRoot(false)
	traceEnabledString := traceEnabled.String()
	traceDisabledString := traceDisabled.String()
	assert.Equal(t,true,traceEnabled.enabled,"Wrong Initialization")
	assert.Equal(t,false,traceDisabled.enabled,"wrong Initialization")
	assert.Equal(t,"message.trace",reflect.TypeOf(traceEnabled).String(),"type mismatch")
	assert.Equal(t,"message.trace",reflect.TypeOf(traceDisabled).String(),"type mismatch")
	assert.Equal(t,"string",reflect.TypeOf(traceEnabledString).String(),"type should be String")
	assert.Equal(t,0, len(traceDisabledString),"No trace should be returned for disabled trace")
	assert.Equal(t,"string",reflect.TypeOf(traceDisabledString).String(),"type should be String")
}

//Tests:
// -newTrace
// - String() for trace path
func Test_newTrace_tracePath_string(t *testing.T){
	messages := []Msg{
		{stageId: 1,processorId: 10,id:7},
		{stageId: 12,processorId: 3,id:2},
		{},
	}
	for _,msg := range messages {
		trace := newTrace(msg)
		assert.Equal(t, true, trace.enabled, "returned trace must be enabled")
		for _, trPath := range trace.path {
			assert.Equal(t, msg.id, trPath.MessageId, "msg Ids must match")
			assert.Equal(t, msg.id, trPath.ProcessorId, "ProcessorIds  must match")
			assert.Equal(t, msg.id, trPath.StageId, "staggeIds  must match")
		}
	}


}