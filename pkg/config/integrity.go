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
	IntegrityCheck        IntegrityCheckType        `toml:"integrity-check" json:"integrity-check"`
	CorruptionHandleLevel CorruptionHandleLevelType `toml:"corruption-handle-option" json:"corruption-handle-option"`
}

// IntegrityCheckType is the level of redo log consistent level.
type IntegrityCheckType string

const (
	// IntegrityCheckNone means no integrity check, the default value.
	IntegrityCheckNone IntegrityCheckType = "none"
	// IntegrityCheckCorrectness means check each row data correctness.
	IntegrityCheckCorrectness IntegrityCheckType = "correctness"
)

type CorruptionHandleLevelType string

const (
	// CorruptionHandleLevelWarn is the default value,
	// log the corrupted event, and mark it as corrupted and send it to the downstream.
	CorruptionHandleLevelWarn CorruptionHandleLevelType = "warn"
	// CorruptionHandleLevelError means log the corrupted event, and then stopped the changefeed.
	CorruptionHandleLevelError CorruptionHandleLevelType = "error"
)

func (t IntegrityCheckType) valid() bool {
	switch t {
	case IntegrityCheckNone, IntegrityCheckCorrectness:
		return true
	default:
	}
	return false
}

func (t CorruptionHandleLevelType) valid() bool {
	switch t {
	case CorruptionHandleLevelWarn, CorruptionHandleLevelError:
		return true
	default:
	}
	return false
}

func (c *IntegrityConfig) Validate() error {
	if !c.IntegrityCheck.valid() {
		return cerror.ErrInvalidReplicaConfig.GenWithStackByArgs()
	}
	if !c.CorruptionHandleLevel.valid() {
		return cerror.ErrInvalidReplicaConfig.GenWithStackByArgs()
	}

	if c.IntegrityCheck == IntegrityCheckCorrectness {
		log.Info("integrity check is enabled, it may affect the performance of the changefeed",
			zap.Any("integrity-check", c.IntegrityCheck),
			zap.Any("corruption-handle-level", c.CorruptionHandleLevel))
	}

	return nil
}
