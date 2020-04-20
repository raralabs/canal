package utils

// JobType represents the kinds of the jobs that is supported.
type JobType uint8

const (
	FILTER JobType = iota + 1 // Filter
	COUNT                     // Counter
	BRANCH                    // Branch
	LUA                       // Lua script
	PASS                      // Just pass

	STDOUT    // Stdout dumper
	BLACKHOLE // Blackhole (messages disappearer)
)
