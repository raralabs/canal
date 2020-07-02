package pipeline

// ProcessorOptions defines various options to be used by
// the processor.
type ProcessorOptions struct {
	Persistor bool // Determines if persistor should be used
}

var DefaultProcessorOptions = ProcessorOptions{
	Persistor: false, // Don't use persistor by default
}
