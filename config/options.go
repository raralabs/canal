package config

import "time"

const (
	DbRoot     = "./tmp/"         // Root of database used by processors
	DefaultTTL = 10 * time.Minute // Default time-to-leave for each entry
)
