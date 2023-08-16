package large_message_handle

import (
	"github.com/pingcap/tiflow/pkg/config"
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

// Config is the configuration for handling large message.
type Config struct {
	LargeMessageHandleOption      string `toml:"large-message-handle-option" json:"large-message-handle-option"`
	LargeMessageHandleCompression string `toml:"large-message-handle-compression" json:"large-message-handle-compression"`
	ClaimCheckStorageURI          string `toml:"claim-check-storage-uri" json:"claim-check-storage-uri"`
}

// NewDefaultConfig return the default Config.
func NewDefaultConfig() *Config {
	return &Config{
		LargeMessageHandleOption:      LargeMessageHandleOptionNone,
		LargeMessageHandleCompression: "none",
	}
}

// Validate the Config.
func (c *Config) Validate(protocol config.Protocol, enableTiDBExtension bool) error {
	if c.LargeMessageHandleOption == LargeMessageHandleOptionNone {
		return nil
	}

	switch protocol {
	case config.ProtocolOpen:
	case config.ProtocolCanalJSON:
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
	}

	if c.LargeMessageHandleCompression != "" {
		if !Supported(c.LargeMessageHandleCompression) {
			return cerror.ErrInvalidReplicaConfig.GenWithStack(
				"large message handle compression is not supported, got %s", c.LargeMessageHandleCompression)
		}
	}
	return nil
}

// HandleKeyOnly returns true if handle large message by encoding handle key only.
func (c *Config) HandleKeyOnly() bool {
	if c == nil {
		return false
	}
	return c.LargeMessageHandleOption == LargeMessageHandleOptionHandleKeyOnly
}

// EnableClaimCheck returns true if enable claim check.
func (c *Config) EnableClaimCheck() bool {
	if c == nil {
		return false
	}
	return c.LargeMessageHandleOption == LargeMessageHandleOptionClaimCheck
}

// Disabled returns true if disable large message handle.
func (c *Config) Disabled() bool {
	if c == nil {
		return false
	}
	return c.LargeMessageHandleOption == LargeMessageHandleOptionNone
}
