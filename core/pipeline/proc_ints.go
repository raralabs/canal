package pipeline

import "github.com/raralabs/canal/core/message"

// IProcessorCommon defines the basic interface for a processor
type IProcessorCommon interface {
	// Error sends the errors produced during execution to the processor
	Error(uint8, error)

	// Done tells the processor that all the execution is done and it should be stopped
	Done()
}

// IProcessorExecutor defines the interface for a processor from the perspective of an Executor.
type IProcessorExecutor interface {
	// An Executor expects a processor to be able to handle the results it produce.
	Result(message.Msg, message.MsgContent)
}

// IProcessorReceiver defines the interface for a processor from the perspective of the message receiver in the same
// stg. It provides methods for a processor to act as an 'Observer'.
type IProcessorReceiver interface {
	// process obtains the incoming message from it's subscription
	process(message.Msg) bool

	// Returns the message routes for the incoming message for processor
	incomingRoutes() msgRoutes

	// lock ...
	lock(msgRoutes)

	// Checks if a processor is closed
	isClosed() bool
}

// IProcessorSender defines the interface for a processor from the perspective of Receiver in the next stg
type IProcessorSender interface {
	// addSendTo adds a send route to a stg from the processor
	addSendTo(stage *stage, route msgRouteParam)

	channelForStageId(stage *stage) <-chan msgPod

	isConnected() bool
}

// IProcessor defines the interface for a processor as a whole
type IProcessor interface {
	IProcessorCommon

	IProcessorExecutor

	IProcessorReceiver

	IProcessorSender

	processorPool() IProcessorPool
}