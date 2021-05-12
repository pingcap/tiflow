// Copyright 2020 PingCAP, Inc.
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
	"encoding/json"
	"fmt"
	"net"
	"strings"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/config/outdated"
	cerror "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/security"
	"go.uber.org/zap"
)

// NewReplicaImpl is true if we using new processor
// new owner should be also switched on after it implemented
const NewReplicaImpl = false

var defaultReplicaConfig = &ReplicaConfig{
	CaseSensitive:    true,
	EnableOldValue:   false,
	CheckGCSafePoint: true,
	Filter: &FilterConfig{
		Rules: []string{"*.*"},
	},
	Mounter: &MounterConfig{
		WorkerNum: 16,
	},
	Sink: &SinkConfig{
		Protocol: "default",
	},
	Cyclic: &CyclicConfig{
		Enable: false,
	},
	Scheduler: &SchedulerConfig{
		Tp:          "table-number",
		PollingTime: -1,
	},
}

// ReplicaConfig represents some addition replication config for a changefeed
type ReplicaConfig replicaConfig

type replicaConfig struct {
	CaseSensitive    bool             `toml:"case-sensitive" json:"case-sensitive"`
	EnableOldValue   bool             `toml:"enable-old-value" json:"enable-old-value"`
	ForceReplicate   bool             `toml:"force-replicate" json:"force-replicate"`
	CheckGCSafePoint bool             `toml:"check-gc-safe-point" json:"check-gc-safe-point"`
	Filter           *FilterConfig    `toml:"filter" json:"filter"`
	Mounter          *MounterConfig   `toml:"mounter" json:"mounter"`
	Sink             *SinkConfig      `toml:"sink" json:"sink"`
	Cyclic           *CyclicConfig    `toml:"cyclic-replication" json:"cyclic-replication"`
	Scheduler        *SchedulerConfig `toml:"scheduler" json:"scheduler"`
}

// Marshal returns the json marshal format of a ReplicationConfig
func (c *ReplicaConfig) Marshal() (string, error) {
	cfg, err := json.Marshal(c)
	if err != nil {
		return "", cerror.WrapError(cerror.ErrEncodeFailed, errors.Annotatef(err, "Unmarshal data: %v", c))
	}
	return string(cfg), nil
}

// Unmarshal unmarshals into *ReplicationConfig from json marshal byte slice
func (c *ReplicaConfig) Unmarshal(data []byte) error {
	return c.UnmarshalJSON(data)
}

// UnmarshalJSON unmarshals into *ReplicationConfig from json marshal byte slice
func (c *ReplicaConfig) UnmarshalJSON(data []byte) error {
	// The purpose of casting ReplicaConfig to replicaConfig is to avoid recursive calls UnmarshalJSON,
	// resulting in stack overflow
	r := (*replicaConfig)(c)
	err := json.Unmarshal(data, &r)
	if err != nil {
		return cerror.WrapError(cerror.ErrDecodeFailed, err)
	}
	v1 := outdated.ReplicaConfigV1{}
	err = v1.Unmarshal(data)
	if err != nil {
		return cerror.WrapError(cerror.ErrDecodeFailed, err)
	}
	r.fillFromV1(&v1)
	return nil
}

// Clone clones a replication
func (c *ReplicaConfig) Clone() *ReplicaConfig {
	str, err := c.Marshal()
	if err != nil {
		log.Panic("failed to marshal replica config",
			zap.Error(cerror.WrapError(cerror.ErrDecodeFailed, err)))
	}
	clone := new(ReplicaConfig)
	err = clone.Unmarshal([]byte(str))
	if err != nil {
		log.Panic("failed to unmarshal replica config",
			zap.Error(cerror.WrapError(cerror.ErrDecodeFailed, err)))
	}
	return clone
}

func (c *replicaConfig) fillFromV1(v1 *outdated.ReplicaConfigV1) {
	if v1 == nil || v1.Sink == nil {
		return
	}
	for _, dispatch := range v1.Sink.DispatchRules {
		c.Sink.DispatchRules = append(c.Sink.DispatchRules, &DispatchRule{
			Matcher:    []string{fmt.Sprintf("%s.%s", dispatch.Schema, dispatch.Name)},
			Dispatcher: dispatch.Rule,
		})
	}
}

// GetDefaultReplicaConfig returns the default replica config
func GetDefaultReplicaConfig() *ReplicaConfig {
	return defaultReplicaConfig.Clone()
}

// SecurityConfig represents security config for server
type SecurityConfig = security.Credential

var defaultServerConfig = &ServerConfig{
	Addr:                   "127.0.0.1:8300",
	AdvertiseAddr:          "",
	LogFile:                "",
	LogLevel:               "info",
	GcTTL:                  24 * 60 * 60, // 24H
	TZ:                     "System",
	OwnerFlushInterval:     TomlDuration(200 * time.Millisecond),
	ProcessorFlushInterval: TomlDuration(100 * time.Millisecond),
	Sorter: &SorterConfig{
		NumConcurrentWorker:    4,
		ChunkSizeLimit:         1024 * 1024 * 1024, // 1GB
		MaxMemoryPressure:      80,
		MaxMemoryConsumption:   8 * 1024 * 1024 * 1024, // 8GB
		NumWorkerPoolGoroutine: 16,
		SortDir:                "/tmp/cdc_sort",
	},
	Security:            &SecurityConfig{},
	PerTableMemoryQuota: 20 * 1024 * 1024, // 20MB
}

// ServerConfig represents a config for server
type ServerConfig struct {
	Addr          string `toml:"addr" json:"addr"`
	AdvertiseAddr string `toml:"advertise-addr" json:"advertise-addr"`

	LogFile  string `toml:"log-file" json:"log-file"`
	LogLevel string `toml:"log-level" json:"log-level"`

	GcTTL int64  `toml:"gc-ttl" json:"gc-ttl"`
	TZ    string `toml:"tz" json:"tz"`

	OwnerFlushInterval     TomlDuration `toml:"owner-flush-interval" json:"owner-flush-interval"`
	ProcessorFlushInterval TomlDuration `toml:"processor-flush-interval" json:"processor-flush-interval"`

	Sorter   *SorterConfig   `toml:"sorter" json:"sorter"`
	Security *SecurityConfig `toml:"security" json:"security"`

	PerTableMemoryQuota uint64 `toml:"per-table-memory-quota" json:"per-table-memory-quota"`
}

// Marshal returns the json marshal format of a ServerConfig
func (c *ServerConfig) Marshal() (string, error) {
	cfg, err := json.Marshal(c)
	if err != nil {
		return "", cerror.WrapError(cerror.ErrEncodeFailed, errors.Annotatef(err, "Unmarshal data: %v", c))
	}
	return string(cfg), nil
}

// Unmarshal unmarshals into *ServerConfig from json marshal byte slice
func (c *ServerConfig) Unmarshal(data []byte) error {
	err := json.Unmarshal(data, c)
	if err != nil {
		return cerror.WrapError(cerror.ErrDecodeFailed, err)
	}
	return nil
}

// String implements the Stringer interface
func (c *ServerConfig) String() string {
	s, _ := c.Marshal()
	return s
}

// Clone clones a replication
func (c *ServerConfig) Clone() *ServerConfig {
	str, err := c.Marshal()
	if err != nil {
		log.Panic("failed to marshal replica config",
			zap.Error(cerror.WrapError(cerror.ErrDecodeFailed, err)))
	}
	clone := new(ServerConfig)
	err = clone.Unmarshal([]byte(str))
	if err != nil {
		log.Panic("failed to unmarshal replica config",
			zap.Error(cerror.WrapError(cerror.ErrDecodeFailed, err)))
	}
	return clone
}

// ValidateAndAdjust validates and adjusts the server configuration
func (c *ServerConfig) ValidateAndAdjust() error {
	if c.Addr == "" {
		return cerror.ErrInvalidServerOption.GenWithStack("empty address")
	}
	if c.AdvertiseAddr == "" {
		c.AdvertiseAddr = c.Addr
	}
	// Advertise address must be specified.
	if idx := strings.LastIndex(c.AdvertiseAddr, ":"); idx >= 0 {
		ip := net.ParseIP(c.AdvertiseAddr[:idx])
		// Skip nil as it could be a domain name.
		if ip != nil && ip.IsUnspecified() {
			return cerror.ErrInvalidServerOption.GenWithStack("advertise address must be specified as a valid IP")
		}
	} else {
		return cerror.ErrInvalidServerOption.GenWithStack("advertise address or address does not contain a port")
	}
	if c.GcTTL == 0 {
		return cerror.ErrInvalidServerOption.GenWithStack("empty GC TTL is not allowed")
	}

	if c.Security != nil && c.Security.IsTLSEnabled() {
		var err error
		_, err = c.Security.ToTLSConfig()
		if err != nil {
			return errors.Annotate(err, "invalidate TLS config")
		}
		_, err = c.Security.ToGRPCDialOption()
		if err != nil {
			return errors.Annotate(err, "invalidate TLS config")
		}
	}

	if c.Sorter == nil {
		c.Sorter = defaultServerConfig.Sorter
	}

	if c.Sorter.ChunkSizeLimit < 1*1024*1024 {
		return cerror.ErrIllegalUnifiedSorterParameter.GenWithStackByArgs("chunk-size-limit should be at least 1MB")
	}
	if c.Sorter.NumConcurrentWorker < 1 {
		return cerror.ErrIllegalUnifiedSorterParameter.GenWithStackByArgs("num-concurrent-worker should be at least 1")
	}
	if c.Sorter.NumWorkerPoolGoroutine > 4096 {
		return cerror.ErrIllegalUnifiedSorterParameter.GenWithStackByArgs("num-workerpool-goroutine should be at most 4096")
	}
	if c.Sorter.NumConcurrentWorker > c.Sorter.NumWorkerPoolGoroutine {
		return cerror.ErrIllegalUnifiedSorterParameter.GenWithStackByArgs("num-concurrent-worker larger than num-workerpool-goroutine is useless")
	}
	if c.Sorter.NumWorkerPoolGoroutine < 1 {
		return cerror.ErrIllegalUnifiedSorterParameter.GenWithStackByArgs("num-workerpool-goroutine should be at least 1, larger than 8 is recommended")
	}
	if c.Sorter.MaxMemoryPressure < 0 || c.Sorter.MaxMemoryPressure > 100 {
		return cerror.ErrIllegalUnifiedSorterParameter.GenWithStackByArgs("max-memory-percentage should be a percentage")
	}

	if c.PerTableMemoryQuota == 0 {
		c.PerTableMemoryQuota = defaultServerConfig.PerTableMemoryQuota
	}
	if c.PerTableMemoryQuota < 6*1024*1024 {
		return cerror.ErrInvalidServerOption.GenWithStackByArgs("per-table-memory-quota should be at least 6MB")
	}

	return nil
}

// GetDefaultServerConfig returns the default server config
func GetDefaultServerConfig() *ServerConfig {
	return defaultServerConfig.Clone()
}

var globalServerConfig atomic.Value

// GetGlobalServerConfig returns the global configuration for this server.
// It should store configuration from command line and configuration file.
// Other parts of the system can read the global configuration use this function.
func GetGlobalServerConfig() *ServerConfig {
	return globalServerConfig.Load().(*ServerConfig)
}

// StoreGlobalServerConfig stores a new config to the globalServerConfig. It mostly uses in the test to avoid some data races.
func StoreGlobalServerConfig(config *ServerConfig) {
	globalServerConfig.Store(config)
}

// TomlDuration is a duration with a custom json decoder and toml decoder
type TomlDuration time.Duration

// UnmarshalText is the toml decoder
func (d *TomlDuration) UnmarshalText(text []byte) error {
	stdDuration, err := time.ParseDuration(string(text))
	if err != nil {
		return err
	}
	*d = TomlDuration(stdDuration)
	return nil
}

// UnmarshalJSON is the json decoder
func (d *TomlDuration) UnmarshalJSON(b []byte) error {
	var stdDuration time.Duration
	if err := json.Unmarshal(b, &stdDuration); err != nil {
		return err
	}
	*d = TomlDuration(stdDuration)
	return nil
}
