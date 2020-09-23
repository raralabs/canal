package pipeline

type ExecutorType uint8

const (
	// Represents the source stages
	// Source stg does not have a receivePool, they don't receive messages from other stages but
	// instead generate those messages.
	SOURCE ExecutorType = iota + 1
	// Represents the Transform stages
	// Transform reads messages from one end and can send msg to other ends.
	// Transform is either an "Do" Transform or an "Agg" Transform.
	TRANSFORM
	// Represents the sink stages.
	// Sinks receive messages but don't send it to other stages.
	SINK
)

// Returns the string form of the ExecutorType
func (et *ExecutorType) String() string {
	switch *et {
	case SOURCE:
		return "SOURCE"
	case TRANSFORM:
		return "TRANSFORM"
	case SINK:
		return "SINK"
	}

	return "UNKNOWN"
}

// An IProcessorForExecutor is a lite version of IProcessor that is designed for the Executor. IProcessor can also be
// passed, wherever IProcessorForExecutor can be passed, to achieve the same result.
type IProcessorForExecutor interface {
	IProcessorCommon

	IProcessorExecutor
}

// An executor interface defines the interface to be followed by an executor.
// Executors in processors includes aggregator functions like
// joiner, counter, tumbling window functions and so on and do functions like
// compute, pass, filter and so on.
type Executor interface {
	Execute(MsgPod, IProcessorForExecutor) bool
	//Execute(message.Msg, IProcessorForExecutor) bool //old implementation of execute . TO roll back just uncomment
	HasLocalState() bool
	ExecutorType() ExecutorType
	SetName(name string)
	Name() string
}
