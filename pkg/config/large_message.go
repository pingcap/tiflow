// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package config

import (
	"github.com/pingcap/tiflow/pkg/compression"
	cerror "github.com/pingcap/tiflow/pkg/errors"
)

const (
	// LargeMessageHandleOptionNone means not handling large message.
	LargeMessageHandleOptionNone string = "none"
	// LargeMessageHandleOptionClaimCheck means handling large message by sending to the claim check storage.
	LargeMessageHandleOptionClaimCheck string = "claim-check"
	// LargeMessageHandleOptionHandleKeyOnly means handling large message by sending only handle key columns.
	LargeMessageHandleOptionHandleKeyOnly string = "handle-key-only"
)

// LargeMessageHandleConfig is the configuration for handling large message.
type LargeMessageHandleConfig struct {
	LargeMessageHandleOption      string `toml:"large-message-handle-option" json:"large-message-handle-option"`
	LargeMessageHandleCompression string `toml:"large-message-handle-compression" json:"large-message-handle-compression"`
	ClaimCheckStorageURI          string `toml:"claim-check-storage-uri" json:"claim-check-storage-uri"`
	ClaimCheckRawValue            bool   `toml:"claim-check-raw-value" json:"claim-check-raw-value"`
}

// NewDefaultLargeMessageHandleConfig return the default Config.
func NewDefaultLargeMessageHandleConfig() *LargeMessageHandleConfig {
	return &LargeMessageHandleConfig{
		LargeMessageHandleOption:      LargeMessageHandleOptionNone,
		LargeMessageHandleCompression: compression.None,
	}
}

// AdjustAndValidate the Config.
func (c *LargeMessageHandleConfig) AdjustAndValidate(protocol Protocol, enableTiDBExtension bool) error {
	if c.LargeMessageHandleOption == "" {
		c.LargeMessageHandleOption = LargeMessageHandleOptionNone
	}

	if c.LargeMessageHandleCompression == "" {
		c.LargeMessageHandleCompression = compression.None
	}

	// compression can be enabled independently
	if !compression.Supported(c.LargeMessageHandleCompression) {
		return cerror.ErrInvalidReplicaConfig.GenWithStack(
			"large message handle compression is not supported, got %s", c.LargeMessageHandleCompression)
	}
	if c.LargeMessageHandleOption == LargeMessageHandleOptionNone {
		return nil
	}

	switch protocol {
	case ProtocolOpen, ProtocolSimple:
	case ProtocolCanalJSON:
		if !enableTiDBExtension {
			return cerror.ErrInvalidReplicaConfig.GenWithStack(
				"large message handle is set to %s, protocol is %s, but enable-tidb-extension is false",
				c.LargeMessageHandleOption, protocol.String())
		}
	default:
		return cerror.ErrInvalidReplicaConfig.GenWithStack(
			"large message handle is set to %s, protocol is %s, it's not supported",
			c.LargeMessageHandleOption, protocol.String())
	}

	if c.LargeMessageHandleOption == LargeMessageHandleOptionClaimCheck {
		if c.ClaimCheckStorageURI == "" {
			return cerror.ErrInvalidReplicaConfig.GenWithStack(
				"large message handle is set to claim-check, but the claim-check-storage-uri is empty")
		}
		if c.ClaimCheckRawValue && protocol == ProtocolOpen {
			return cerror.ErrInvalidReplicaConfig.GenWithStack(
				"large message handle is set to claim-check, raw value is not supported for the open protocol")
		}
	}

	return nil
}

// HandleKeyOnly returns true if handle large message by encoding handle key only.
func (c *LargeMessageHandleConfig) HandleKeyOnly() bool {
	if c == nil {
		return false
	}
	return c.LargeMessageHandleOption == LargeMessageHandleOptionHandleKeyOnly
}

// EnableClaimCheck returns true if enable claim check.
func (c *LargeMessageHandleConfig) EnableClaimCheck() bool {
	if c == nil {
		return false
	}
	return c.LargeMessageHandleOption == LargeMessageHandleOptionClaimCheck
}

// Disabled returns true if disable large message handle.
func (c *LargeMessageHandleConfig) Disabled() bool {
	if c == nil {
		return false
	}
	return c.LargeMessageHandleOption == LargeMessageHandleOptionNone
}
