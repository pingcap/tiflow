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
	"math"
	"net"
	"regexp"
	"strings"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/security"
	"go.uber.org/zap"
)

const (
	// clusterIDMaxLen is the max length of cdc server cluster id
	clusterIDMaxLen = 128
	// DefaultSortDir is the default value of sort-dir, it will be a subordinate directory of data-dir.
	DefaultSortDir = "/tmp/sorter"

	// DefaultRedoDir is a subordinate directory path of data-dir.
	DefaultRedoDir = "/tmp/redo"

	// DebugConfigurationItem is the name of debug configurations
	DebugConfigurationItem = "debug"

	// DefaultChangefeedMemoryQuota is the default memory quota for each changefeed.
	DefaultChangefeedMemoryQuota = 1024 * 1024 * 1024 // 1GB.

	// DisableMemoryLimit is the default max memory percentage for TiCDC server.
	// 0 means no memory limit.
	DisableMemoryLimit = 0
)

var (
	clusterIDRe = regexp.MustCompile(`^[a-zA-Z0-9]+(-[a-zA-Z0-9]+)*$`)

	// ReservedClusterIDs contains a list of reserved cluster id,
	// these words are the part of old cdc etcd key prefix
	// like: /tidb/cdc/owner
	ReservedClusterIDs = []string{
		"owner", "capture", "task",
		"changefeed", "job", "meta",
	}
)

func init() {
	StoreGlobalServerConfig(GetDefaultServerConfig())
}

// SecurityConfig represents security config for server
type SecurityConfig = security.Credential

// LogFileConfig represents log file config for server
type LogFileConfig struct {
	MaxSize    int `toml:"max-size" json:"max-size"`
	MaxDays    int `toml:"max-days" json:"max-days"`
	MaxBackups int `toml:"max-backups" json:"max-backups"`
}

// LogConfig represents log config for server
type LogConfig struct {
	File              *LogFileConfig `toml:"file" json:"file"`
	InternalErrOutput string         `toml:"error-output" json:"error-output"`
}

var defaultServerConfig = &ServerConfig{
	Addr:          "127.0.0.1:8300",
	AdvertiseAddr: "",
	LogFile:       "",
	LogLevel:      "info",
	Log: &LogConfig{
		File: &LogFileConfig{
			MaxSize:    300,
			MaxDays:    0,
			MaxBackups: 0,
		},
		InternalErrOutput: "stderr",
	},
	DataDir: "",
	GcTTL:   24 * 60 * 60, // 24H
	TZ:      "System",
	// The default election-timeout in PD is 3s and minimum session TTL is 5s,
	// which is calculated by `math.Ceil(3 * election-timeout / 2)`, we choose
	// default capture session ttl to 10s to increase robust to PD jitter,
	// however it will decrease RTO when single TiCDC node error happens.
	CaptureSessionTTL:      10,
	OwnerFlushInterval:     TomlDuration(50 * time.Millisecond),
	ProcessorFlushInterval: TomlDuration(50 * time.Millisecond),
	Sorter: &SorterConfig{
		SortDir:             DefaultSortDir,
		CacheSizeInMB:       128, // By default use 128M memory as sorter cache.
		MaxMemoryPercentage: 10,  // Deprecated.
	},
	Security: &SecurityConfig{},
	KVClient: &KVClientConfig{
		WorkerConcurrent: 8,
		WorkerPoolSize:   0, // 0 will use NumCPU() * 2
		RegionScanLimit:  40,
		// The default TiKV region election timeout is [10s, 20s],
		// Use 1 minute to cover region leader missing.
		RegionRetryDuration: TomlDuration(time.Minute),
	},
	Debug: &DebugConfig{
		DB: &DBConfig{
			Count: 8,
			// Following configs are optimized for write/read throughput.
			// Users should not change them.
			Concurrency:                 128,
			MaxOpenFiles:                10000,
			BlockSize:                   65536,
			WriterBufferSize:            8388608,
			Compression:                 "snappy",
			WriteL0PauseTrigger:         math.MaxInt32,
			CompactionL0Trigger:         160,
			CompactionDeletionThreshold: 10485760,
			CompactionPeriod:            1800,
			IteratorMaxAliveDuration:    10000,
			IteratorSlowReadDuration:    256,
		},
		Messages: defaultMessageConfig.Clone(),

		Scheduler:              NewDefaultSchedulerConfig(),
		EnableKVConnectBackOff: false,
	},
	ClusterID:              "default",
	GcTunerMemoryThreshold: DisableMemoryLimit,
}

// ServerConfig represents a config for server
type ServerConfig struct {
	Addr          string `toml:"addr" json:"addr"`
	AdvertiseAddr string `toml:"advertise-addr" json:"advertise-addr"`

	LogFile  string     `toml:"log-file" json:"log-file"`
	LogLevel string     `toml:"log-level" json:"log-level"`
	Log      *LogConfig `toml:"log" json:"log"`

	DataDir string `toml:"data-dir" json:"data-dir"`

	GcTTL int64  `toml:"gc-ttl" json:"gc-ttl"`
	TZ    string `toml:"tz" json:"tz"`

	CaptureSessionTTL int `toml:"capture-session-ttl" json:"capture-session-ttl"`

	OwnerFlushInterval     TomlDuration `toml:"owner-flush-interval" json:"owner-flush-interval"`
	ProcessorFlushInterval TomlDuration `toml:"processor-flush-interval" json:"processor-flush-interval"`

	Sorter   *SorterConfig   `toml:"sorter" json:"sorter"`
	Security *SecurityConfig `toml:"security" json:"security"`
	// DEPRECATED: after using pull based sink, this config is useless.
	// Because we do not control the memory usage by table anymore.
	PerTableMemoryQuota uint64          `toml:"per-table-memory-quota" json:"per-table-memory-quota"`
	KVClient            *KVClientConfig `toml:"kv-client" json:"kv-client"`
	Debug               *DebugConfig    `toml:"debug" json:"debug"`
	ClusterID           string          `toml:"cluster-id" json:"cluster-id"`
	// Deprecated: we don't use this field anymore.
	MaxMemoryPercentage    int    `toml:"max-memory-percentage" json:"max-memory-percentage"`
	GcTunerMemoryThreshold uint64 `toml:"gc-tuner-memory-threshold" json:"gc-tuner-memory-threshold"`
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
	if !isValidClusterID(c.ClusterID) {
		return cerror.ErrInvalidServerOption.GenWithStack(fmt.Sprintf("bad cluster-id"+
			"please match the pattern \"^[a-zA-Z0-9]+(\\-[a-zA-Z0-9]+)*$\", and not the list of"+
			" following reserved world: %s"+
			"eg, \"simple-cluster-id\"", strings.Join(ReservedClusterIDs, ",")))
	}
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
	// 5s is minimum lease ttl in etcd(PD)
	if c.CaptureSessionTTL < 5 {
		log.Warn("capture session ttl too small, set to default value 10s")
		c.CaptureSessionTTL = 10
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

	defaultCfg := GetDefaultServerConfig()
	if c.Sorter == nil {
		c.Sorter = defaultCfg.Sorter
	}
	c.Sorter.SortDir = DefaultSortDir
	err := c.Sorter.ValidateAndAdjust()
	if err != nil {
		return err
	}

	if c.KVClient == nil {
		c.KVClient = defaultCfg.KVClient
	}
	if err = c.KVClient.ValidateAndAdjust(); err != nil {
		return errors.Trace(err)
	}

	if c.Debug == nil {
		c.Debug = defaultCfg.Debug
	}
	if err = c.Debug.ValidateAndAdjust(); err != nil {
		return errors.Trace(err)
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

// isValidClusterID returns true if the cluster ID matches
// the pattern "^[a-zA-Z0-9]+(\-[a-zA-Z0-9]+)*$", length no more than `clusterIDMaxLen`,
// eg, "simple-cluster-id".
func isValidClusterID(clusterID string) bool {
	valid := clusterID != "" && len(clusterID) <= clusterIDMaxLen &&
		clusterIDRe.MatchString(clusterID)
	if !valid {
		return false
	}
	for _, reserved := range ReservedClusterIDs {
		if reserved == clusterID {
			return false
		}
	}
	return true
}
