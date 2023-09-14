// Copyright 2023 PingCAP, Inc.
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

import cerror "github.com/pingcap/tiflow/pkg/errors"

const (
	// LargeMessageHandleOptionNone means not handling large message.
	LargeMessageHandleOptionNone string = "none"
	// LargeMessageHandleOptionHandleKeyOnly means handling large message by sending only handle key columns.
	LargeMessageHandleOptionHandleKeyOnly string = "handle-key-only"
)

// LargeMessageHandleConfig is the configuration for handling large message.
type LargeMessageHandleConfig struct {
	LargeMessageHandleOption string `toml:"large-message-handle-option" json:"large-message-handle-option"`
}

// NewDefaultLargeMessageHandleConfig return the default LargeMessageHandleConfig.
func NewDefaultLargeMessageHandleConfig() *LargeMessageHandleConfig {
	return &LargeMessageHandleConfig{
		LargeMessageHandleOption: LargeMessageHandleOptionNone,
	}
}

// AdjustAndValidate the LargeMessageHandleConfig.
func (c *LargeMessageHandleConfig) AdjustAndValidate(protocol Protocol, enableTiDBExtension bool) error {
	if c.LargeMessageHandleOption == "" {
		c.LargeMessageHandleOption = LargeMessageHandleOptionNone
	}
	if c.LargeMessageHandleOption == LargeMessageHandleOptionNone {
		return nil
	}

	switch protocol {
	case ProtocolOpen:
		return nil
	case ProtocolCanalJSON:
		if !enableTiDBExtension {
			return cerror.ErrInvalidReplicaConfig.GenWithStack(
				"large message handle is set to %s, protocol is %s, but enable-tidb-extension is false",
				c.LargeMessageHandleOption, protocol.String())
		}
		return nil
	default:
	}
	return cerror.ErrInvalidReplicaConfig.GenWithStack(
		"large message handle is set to %s, protocol is %s, it's not supported",
		c.LargeMessageHandleOption, protocol.String())
}

// HandleKeyOnly returns true if handle large message by encoding handle key only.
func (c *LargeMessageHandleConfig) HandleKeyOnly() bool {
	if c == nil {
		return false
	}
	return c.LargeMessageHandleOption == LargeMessageHandleOptionHandleKeyOnly
}

// Disabled returns true if disable large message handle.
func (c *LargeMessageHandleConfig) Disabled() bool {
	if c == nil {
		return false
	}
	return c.LargeMessageHandleOption == LargeMessageHandleOptionNone
}
