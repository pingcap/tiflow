// Copyright 2021 PingCAP, Inc.
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

package config

import (
	"github.com/pingcap/log"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"go.uber.org/zap"
)

// IntegrityConfig represents integrity check config for a changefeed.
type IntegrityConfig struct {
	IntegrityCheckLevel   string `toml:"integrity-check-level" json:"integrity-check-level"`
	CorruptionHandleLevel string `toml:"corruption-handle-level" json:"corruption-handle-level"`
}

const (
	// IntegrityCheckLevelNone means no integrity check, the default value.
	IntegrityCheckLevelNone string = "none"
	// IntegrityCheckLevelCorrectness means check each row data correctness.
	IntegrityCheckLevelCorrectness string = "correctness"
)

const (
	// CorruptionHandleLevelWarn is the default value,
	// log the corrupted event, and mark it as corrupted and send it to the downstream.
	CorruptionHandleLevelWarn string = "warn"
	// CorruptionHandleLevelError means log the corrupted event, and then stopped the changefeed.
	CorruptionHandleLevelError string = "error"
)

//func (t IntegrityCheckLevelType) valid() bool {
//	switch t {
//	case IntegrityCheckLevelNone, IntegrityCheckLevelCorrectness:
//		return true
//	default:
//	}
//	return false
//}
//
//func (t CorruptionHandleLevelType) valid() bool {
//	switch t {
//	case CorruptionHandleLevelWarn, CorruptionHandleLevelError:
//		return true
//	default:
//	}
//	return false
//}

// Validate the integrity config.
func (c *IntegrityConfig) Validate() error {
	if c.IntegrityCheckLevel != IntegrityCheckLevelNone &&
		c.IntegrityCheckLevel != IntegrityCheckLevelCorrectness {
		return cerror.ErrInvalidReplicaConfig.GenWithStackByArgs()
	}
	if c.CorruptionHandleLevel != CorruptionHandleLevelWarn &&
		c.CorruptionHandleLevel != CorruptionHandleLevelError {
		return cerror.ErrInvalidReplicaConfig.GenWithStackByArgs()
	}

	if c.Enabled() {
		log.Info("integrity check is enabled",
			zap.Any("integrityCheckLevel", c.IntegrityCheckLevel),
			zap.Any("corruptionHandleLevel", c.CorruptionHandleLevel))
	}

	return nil
}

// Enabled returns true if the integrity check is enabled.
func (c *IntegrityConfig) Enabled() bool {
	return c.IntegrityCheckLevel == IntegrityCheckLevelCorrectness
}

// ErrorHandle returns true if the corruption handle level is error.
func (c *IntegrityConfig) ErrorHandle() bool {
	return c.CorruptionHandleLevel == CorruptionHandleLevelError
}
