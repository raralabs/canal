package pipeline

import (
	"github.com/raralabs/canal/core/message"
	"github.com/raralabs/canal/core/message/content"
)

// IProcessorCommon defines the basic interface for a processor
type IProcessorCommon interface {
	// Error sends the errors produced during execution to the processor.
	// The processor is responsible for properly routing the error messages.
	Error(uint8, error)

	// Done tells the processor that all the execution is done and it should be stopped.
	// It checks if the processor is already closed, and if it is not closed, closes it.
	Done()

	// Checks if a processor is closed.
	IsClosed() bool
}

// IProcessorExecutor defines the interface for a processor from the perspective of an Executor.
type IProcessorExecutor interface {
	// An Executor expects a processor to be able to handle the results it produce.
	Result(m message.Msg, content, pContent content.IContent)

	// Returns the persistor held by the processor.
	Persistor() IPersistor
}

// IProcessorReceiver defines the interface for a processor from the perspective of the message receiver in the same
// stg. It provides methods for a processor to act as an 'Observer'.
type IProcessorReceiver interface {
	// process obtains the incoming message from it's subscription.
	process(message.Msg) bool

	// Returns the message routes for the incoming message for processor.
	incomingRoutes() msgRoutes

	// lock ...
	lock(msgRoutes)
}

// IProcessorSender defines the interface for a processor from the perspective of Receiver in the next stg
type IProcessorSender interface {
	// addSendTo adds a send route to a stg from the processor.
	addSendTo(stage *stage, route MsgRouteParam)

	// channelForStageId returns a channel that emits message for the respective stage.
	channelForStageId(stage *stage) <-chan msgPod

	// isConnected tells if the processor is properly connected to the receiver in the next stage.
	// For a sink type processor, it simply returns true.
	isConnected() bool
}

// IProcessor defines the interface for a processor as a whole.
// A processor is made up of four major interfaces:
// 		- The basic and common interfaces that performs task like error reporting, closing processor and so on
//		- The interfaces that Executor relies on to send results of it's execution to the processor
//		- The receiver interface that receives messages from pool. It is also responsible for acting as an 'Observer',
//		  so that it can receive messages from only the routes it needed.
//		- The sender interface to send out the messages it produce. The next stage relies on this interface to obtain
//		  messages appropriately.
type IProcessor interface {
	IProcessorCommon

	IProcessorExecutor

	IProcessorReceiver

	IProcessorSender

	// processorPool returns the processor pool, to which the IProcessor is attached to.
	processorPool() IProcessorPool

	// metadata gives the metadata produced during the execution of processor.
	metadata() *metadata
}
