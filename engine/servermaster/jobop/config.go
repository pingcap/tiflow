// Copyright 2022 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package jobop

import "time"

const (
	defaultBackoffInitInterval = 5 * time.Second
	defaultBackoffMaxInterval  = 5 * time.Minute
	defaultBackoffMultiplier   = 1.2
	// If a job can keep running for more than 10 minutes, it won't be backoff
	// If a job keeps failing, the max back interval is 5 minutes, and 10 minutes
	// can keep at least one failed record.
	defaultBackoffResetInterval = 2 * defaultBackoffMaxInterval
	// with 1.2 as multiplier, it will cost approximately 32 minutes to reach max interval 5min,
	// and it will keep trying for every 5min until approximate 48 hours. Then, it will quit.
	defaultBackoffMaxTryTime = 600
)

// BackoffConfig is used to configure job backoff
type BackoffConfig struct {
	ResetInterval   time.Duration `toml:"reset-interval" json:"reset-interval"`
	InitialInterval time.Duration `toml:"initial-interval" json:"initial-interval"`
	MaxInterval     time.Duration `toml:"max-interval" json:"max-interval"`
	Multiplier      float64       `toml:"multiplier" json:"multiplier"`
	MaxTryTime      int           `toml:"max-try-time" json:"max-try-time"`
}

// NewDefaultBackoffConfig creates a default backoff config
func NewDefaultBackoffConfig() *BackoffConfig {
	return &BackoffConfig{
		ResetInterval:   defaultBackoffResetInterval,
		InitialInterval: defaultBackoffInitInterval,
		MaxInterval:     defaultBackoffMaxInterval,
		Multiplier:      defaultBackoffMultiplier,
		MaxTryTime:      defaultBackoffMaxTryTime,
	}
}
