package config

import "time"

const (
	DB_ROOT     = "./"             // Root of database used by processors
	DEFAULT_TTL = 10 * time.Minute // Default time-to-leave for each entry
)
