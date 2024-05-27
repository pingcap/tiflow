// Copyright 2019 PingCAP, Inc.
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
	"flag"
	"fmt"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/coreos/go-semver/semver"
	"github.com/docker/go-units"
	"github.com/dustin/go-humanize"
	"github.com/pingcap/tidb/pkg/lightning/config"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/util/filter"
	router "github.com/pingcap/tidb/pkg/util/table-router"
	"github.com/pingcap/tiflow/dm/config/dbconfig"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/dm/pkg/terror"
	"github.com/pingcap/tiflow/dm/pkg/utils"
	bf "github.com/pingcap/tiflow/pkg/binlog-filter"
	"github.com/pingcap/tiflow/pkg/column-mapping"
	"go.uber.org/zap"
	"gopkg.in/yaml.v2"
)

// Online DDL Scheme.
const (
	GHOST = "gh-ost"
	PT    = "pt"
)

// shard DDL mode.
const (
	ShardPessimistic  = "pessimistic"
	ShardOptimistic   = "optimistic"
	tidbTxnMode       = "tidb_txn_mode"
	tidbTxnOptimistic = "optimistic"
)

// collation_compatible.
const (
	LooseCollationCompatible  = "loose"
	StrictCollationCompatible = "strict"
)

const (
	ValidationNone = "none"
	ValidationFast = "fast"
	ValidationFull = "full"

	DefaultValidatorWorkerCount       = 4
	DefaultValidatorValidateInterval  = 10 * time.Second
	DefaultValidatorCheckInterval     = 5 * time.Second
	DefaultValidatorRowErrorDelay     = 30 * time.Minute
	DefaultValidatorMetaFlushInterval = 5 * time.Minute
	DefaultValidatorBatchQuerySize    = 100
	DefaultValidatorMaxPendingRowSize = "500m"

	ValidatorMaxAccumulatedRow = 100000
	// PendingRow is substantial in this version (in sysbench test)
	// set to MaxInt temporaly and reset in the future.
	DefaultValidatorMaxPendingRow = math.MaxInt32
)

// default config item values.
var (
	// TaskConfig.
	defaultMetaSchema          = "dm_meta"
	defaultEnableHeartbeat     = false
	defaultIsSharding          = false
	defaultUpdateInterval      = 1
	defaultReportInterval      = 10
	defaultCollationCompatible = "loose"
	// MydumperConfig.
	defaultMydumperPath  = "./bin/mydumper"
	defaultThreads       = 4
	defaultChunkFilesize = "64"
	defaultSkipTzUTC     = true
	// LoaderConfig.
	defaultPoolSize = 16
	defaultDir      = "./dumped_data"
	// SyncerConfig.
	defaultWorkerCount             = 16
	defaultBatch                   = 100
	defaultQueueSize               = 1024 // do not give too large default value to avoid OOM
	defaultCheckpointFlushInterval = 30   // in seconds
	defaultSafeModeDuration        = strconv.Itoa(2*defaultCheckpointFlushInterval) + "s"

	// TargetDBConfig.
	defaultSessionCfg = []struct {
		key        string
		val        string
		minVersion *semver.Version
	}{
		{tidbTxnMode, tidbTxnOptimistic, semver.New("3.0.0")},
	}
)

// Meta represents binlog's meta pos
// NOTE: refine to put these config structs into pkgs
// NOTE: now, syncer does not support GTID mode and which is supported by relay.
type Meta struct {
	BinLogName string `toml:"binlog-name" yaml:"binlog-name"`
	BinLogPos  uint32 `toml:"binlog-pos" yaml:"binlog-pos"`
	BinLogGTID string `toml:"binlog-gtid" yaml:"binlog-gtid"`
}

// Verify does verification on configs
// NOTE: we can't decide to verify `binlog-name` or `binlog-gtid` until being bound to a source (with `enable-gtid` set).
func (m *Meta) Verify() error {
	if m != nil && len(m.BinLogName) == 0 && len(m.BinLogGTID) == 0 {
		return terror.ErrConfigMetaInvalid.Generate()
	}

	return nil
}

// MySQLInstance represents a sync config of a MySQL instance.
type MySQLInstance struct {
	// it represents a MySQL/MariaDB instance or a replica group
	SourceID    string   `yaml:"source-id"`
	Meta        *Meta    `yaml:"meta"`
	FilterRules []string `yaml:"filter-rules"`
	// deprecated
	ColumnMappingRules []string `yaml:"column-mapping-rules"`
	RouteRules         []string `yaml:"route-rules"`
	ExpressionFilters  []string `yaml:"expression-filters"`

	// black-white-list is deprecated, use block-allow-list instead
	BWListName string `yaml:"black-white-list"`
	BAListName string `yaml:"block-allow-list"`

	MydumperConfigName string          `yaml:"mydumper-config-name"`
	Mydumper           *MydumperConfig `yaml:"mydumper"`
	// MydumperThread is alias for Threads in MydumperConfig, and its priority is higher than Threads
	MydumperThread int `yaml:"mydumper-thread"`

	LoaderConfigName string        `yaml:"loader-config-name"`
	Loader           *LoaderConfig `yaml:"loader"`
	// LoaderThread is alias for PoolSize in LoaderConfig, and its priority is higher than PoolSize
	LoaderThread int `yaml:"loader-thread"`

	SyncerConfigName string        `yaml:"syncer-config-name"`
	Syncer           *SyncerConfig `yaml:"syncer"`
	// SyncerThread is alias for WorkerCount in SyncerConfig, and its priority is higher than WorkerCount
	SyncerThread int `yaml:"syncer-thread"`

	ContinuousValidatorConfigName string          `yaml:"validator-config-name"`
	ContinuousValidator           ValidatorConfig `yaml:"-"`
}

// VerifyAndAdjust does verification on configs, and adjust some configs.
func (m *MySQLInstance) VerifyAndAdjust() error {
	if m == nil {
		return terror.ErrConfigMySQLInstNotFound.Generate()
	}

	if m.SourceID == "" {
		return terror.ErrConfigEmptySourceID.Generate()
	}

	if err := m.Meta.Verify(); err != nil {
		return terror.Annotatef(err, "source %s", m.SourceID)
	}

	if len(m.MydumperConfigName) > 0 && m.Mydumper != nil {
		return terror.ErrConfigMydumperCfgConflict.Generate()
	}
	if len(m.LoaderConfigName) > 0 && m.Loader != nil {
		return terror.ErrConfigLoaderCfgConflict.Generate()
	}
	if len(m.SyncerConfigName) > 0 && m.Syncer != nil {
		return terror.ErrConfigSyncerCfgConflict.Generate()
	}

	if len(m.BAListName) == 0 && len(m.BWListName) != 0 {
		m.BAListName = m.BWListName
	}

	return nil
}

// MydumperConfig represents mydumper process unit's specific config.
type MydumperConfig struct {
	MydumperPath  string `yaml:"mydumper-path" toml:"mydumper-path" json:"mydumper-path"`    // mydumper binary path
	Threads       int    `yaml:"threads" toml:"threads" json:"threads"`                      // -t, --threads
	ChunkFilesize string `yaml:"chunk-filesize" toml:"chunk-filesize" json:"chunk-filesize"` // -F, --chunk-filesize
	StatementSize uint64 `yaml:"statement-size" toml:"statement-size" json:"statement-size"` // -S, --statement-size
	Rows          uint64 `yaml:"rows" toml:"rows" json:"rows"`                               // -r, --rows
	Where         string `yaml:"where" toml:"where" json:"where"`                            // --where

	SkipTzUTC bool   `yaml:"skip-tz-utc" toml:"skip-tz-utc" json:"skip-tz-utc"` // --skip-tz-utc
	ExtraArgs string `yaml:"extra-args" toml:"extra-args" json:"extra-args"`    // other extra args
	// NOTE: use LoaderConfig.Dir as --outputdir
	// TODO zxc: combine -B -T --regex with filter rules?
}

// DefaultMydumperConfig return default mydumper config for task.
func DefaultMydumperConfig() MydumperConfig {
	return MydumperConfig{
		MydumperPath:  defaultMydumperPath,
		Threads:       defaultThreads,
		ChunkFilesize: defaultChunkFilesize,
		SkipTzUTC:     defaultSkipTzUTC,
	}
}

// alias to avoid infinite recursion for UnmarshalYAML.
type rawMydumperConfig MydumperConfig

// UnmarshalYAML implements Unmarshaler.UnmarshalYAML.
func (m *MydumperConfig) UnmarshalYAML(unmarshal func(interface{}) error) error {
	raw := rawMydumperConfig(DefaultMydumperConfig())
	if err := unmarshal(&raw); err != nil {
		return terror.ErrConfigYamlTransform.Delegate(err, "unmarshal mydumper config")
	}
	*m = MydumperConfig(raw) // raw used only internal, so no deep copy
	return nil
}

// LoadMode defines different mode used in load phase.
type LoadMode string

const (
	// LoadModeSQL means write data by sql statements, uses tidb-lightning tidb backend to load data.
	// deprecated, use LoadModeLogical instead.
	LoadModeSQL LoadMode = "sql"
	// LoadModeLoader is the legacy sql mode, use loader to load data. this should be replaced by LoadModeLogical mode.
	// deprecated, use LoadModeLogical instead.
	LoadModeLoader LoadMode = "loader"
	// LoadModeLogical means use tidb backend of lightning to load data, which uses SQL to load data.
	LoadModeLogical LoadMode = "logical"
	// LoadModePhysical means use local backend of lightning to load data, which ingest SST files to load data.
	LoadModePhysical LoadMode = "physical"
)

// LogicalDuplicateResolveType defines the duplication resolution when meet duplicate rows for logical import.
type LogicalDuplicateResolveType string

const (
	// OnDuplicateReplace represents replace the old row with new data.
	OnDuplicateReplace LogicalDuplicateResolveType = "replace"
	// OnDuplicateError represents return an error when meet duplicate row.
	OnDuplicateError LogicalDuplicateResolveType = "error"
	// OnDuplicateIgnore represents ignore the new data when meet duplicate row.
	OnDuplicateIgnore LogicalDuplicateResolveType = "ignore"
)

// PhysicalDuplicateResolveType defines the duplication resolution when meet duplicate rows for physical import.
type PhysicalDuplicateResolveType string

const (
	// OnDuplicateNone represents do nothing when meet duplicate row and the task will continue.
	OnDuplicateNone PhysicalDuplicateResolveType = "none"
	// OnDuplicateManual represents that task should be paused when meet duplicate row to let user handle it manually.
	OnDuplicateManual PhysicalDuplicateResolveType = "manual"
)

// PhysicalPostOpLevel defines the configuration of checksum/analyze of physical import.
type PhysicalPostOpLevel string

const (
	OpLevelRequired = "required"
	OpLevelOptional = "optional"
	OpLevelOff      = "off"
)

// LoaderConfig represents loader process unit's specific config.
type LoaderConfig struct {
	PoolSize           int      `yaml:"pool-size" toml:"pool-size" json:"pool-size"`
	Dir                string   `yaml:"dir" toml:"dir" json:"dir"`
	SortingDirPhysical string   `yaml:"sorting-dir-physical" toml:"sorting-dir-physical" json:"sorting-dir-physical"`
	SQLMode            string   `yaml:"-" toml:"-" json:"-"` // wrote by dump unit (DM op) or jobmaster (DM in engine)
	ImportMode         LoadMode `yaml:"import-mode" toml:"import-mode" json:"import-mode"`
	// deprecated, use OnDuplicateLogical instead.
	OnDuplicate         LogicalDuplicateResolveType  `yaml:"on-duplicate" toml:"on-duplicate" json:"on-duplicate"`
	OnDuplicateLogical  LogicalDuplicateResolveType  `yaml:"on-duplicate-logical" toml:"on-duplicate-logical" json:"on-duplicate-logical"`
	OnDuplicatePhysical PhysicalDuplicateResolveType `yaml:"on-duplicate-physical" toml:"on-duplicate-physical" json:"on-duplicate-physical"`
	DiskQuotaPhysical   config.ByteSize              `yaml:"disk-quota-physical" toml:"disk-quota-physical" json:"disk-quota-physical"`
	ChecksumPhysical    PhysicalPostOpLevel          `yaml:"checksum-physical" toml:"checksum-physical" json:"checksum-physical"`
	Analyze             PhysicalPostOpLevel          `yaml:"analyze" toml:"analyze" json:"analyze"`
	RangeConcurrency    int                          `yaml:"range-concurrency" toml:"range-concurrency" json:"range-concurrency"`
	CompressKVPairs     string                       `yaml:"compress-kv-pairs" toml:"compress-kv-pairs" json:"compress-kv-pairs"`
	PDAddr              string                       `yaml:"pd-addr" toml:"pd-addr" json:"pd-addr"`
}

// DefaultLoaderConfig return default loader config for task.
func DefaultLoaderConfig() LoaderConfig {
	return LoaderConfig{
		PoolSize:           defaultPoolSize,
		Dir:                defaultDir,
		ImportMode:         LoadModeLogical,
		OnDuplicateLogical: OnDuplicateReplace,
	}
}

// alias to avoid infinite recursion for UnmarshalYAML.
type rawLoaderConfig LoaderConfig

// UnmarshalYAML implements Unmarshaler.UnmarshalYAML.
func (m *LoaderConfig) UnmarshalYAML(unmarshal func(interface{}) error) error {
	raw := rawLoaderConfig(DefaultLoaderConfig())
	if err := unmarshal(&raw); err != nil {
		return terror.ErrConfigYamlTransform.Delegate(err, "unmarshal loader config")
	}
	*m = LoaderConfig(raw) // raw used only internal, so no deep copy
	return nil
}

func (m *LoaderConfig) adjust() error {
	if m.ImportMode == "" {
		m.ImportMode = LoadModeLogical
	}
	if strings.EqualFold(string(m.ImportMode), string(LoadModeSQL)) ||
		strings.EqualFold(string(m.ImportMode), string(LoadModeLoader)) {
		m.ImportMode = LoadModeLogical
	}
	m.ImportMode = LoadMode(strings.ToLower(string(m.ImportMode)))
	switch m.ImportMode {
	case LoadModeLoader, LoadModeSQL, LoadModeLogical, LoadModePhysical:
	default:
		return terror.ErrConfigInvalidLoadMode.Generate(m.ImportMode)
	}

	if m.PoolSize == 0 {
		m.PoolSize = defaultPoolSize
	}

	if m.OnDuplicateLogical == "" {
		m.OnDuplicateLogical = OnDuplicateReplace
	}
	m.OnDuplicateLogical = LogicalDuplicateResolveType(strings.ToLower(string(m.OnDuplicateLogical)))
	switch m.OnDuplicateLogical {
	case OnDuplicateReplace, OnDuplicateError, OnDuplicateIgnore:
	default:
		return terror.ErrConfigInvalidDuplicateResolution.Generate(m.OnDuplicateLogical)
	}

	if m.OnDuplicatePhysical == "" {
		m.OnDuplicatePhysical = OnDuplicateNone
	}
	m.OnDuplicatePhysical = PhysicalDuplicateResolveType(strings.ToLower(string(m.OnDuplicatePhysical)))
	switch m.OnDuplicatePhysical {
	case OnDuplicateNone, OnDuplicateManual:
	default:
		return terror.ErrConfigInvalidPhysicalDuplicateResolution.Generate(m.OnDuplicatePhysical)
	}

	if m.ChecksumPhysical == "" {
		m.ChecksumPhysical = OpLevelRequired
	}
	m.ChecksumPhysical = PhysicalPostOpLevel(strings.ToLower(string(m.ChecksumPhysical)))
	switch m.ChecksumPhysical {
	case OpLevelRequired, OpLevelOptional, OpLevelOff:
	default:
		return terror.ErrConfigInvalidPhysicalChecksum.Generate(m.ChecksumPhysical)
	}

	if m.Analyze == "" {
		m.Analyze = OpLevelOptional
	}
	m.Analyze = PhysicalPostOpLevel(strings.ToLower(string(m.Analyze)))
	switch m.Analyze {
	case OpLevelRequired, OpLevelOptional, OpLevelOff:
	default:
		return terror.ErrConfigInvalidLoadAnalyze.Generate(m.Analyze)
	}

	return nil
}

// SyncerConfig represents syncer process unit's specific config.
type SyncerConfig struct {
	MetaFile    string `yaml:"meta-file" toml:"meta-file" json:"meta-file"` // meta filename, used only when load SubConfig directly
	WorkerCount int    `yaml:"worker-count" toml:"worker-count" json:"worker-count"`
	Batch       int    `yaml:"batch" toml:"batch" json:"batch"`
	QueueSize   int    `yaml:"queue-size" toml:"queue-size" json:"queue-size"`
	// checkpoint flush interval in seconds.
	CheckpointFlushInterval int `yaml:"checkpoint-flush-interval" toml:"checkpoint-flush-interval" json:"checkpoint-flush-interval"`
	// TODO: add this two new config items for openapi.
	Compact      bool `yaml:"compact" toml:"compact" json:"compact"`
	MultipleRows bool `yaml:"multiple-rows" toml:"multiple-rows" json:"multiple-rows"`

	// deprecated
	MaxRetry int `yaml:"max-retry" toml:"max-retry" json:"max-retry"`

	// deprecated
	AutoFixGTID bool `yaml:"auto-fix-gtid" toml:"auto-fix-gtid" json:"auto-fix-gtid"`
	EnableGTID  bool `yaml:"enable-gtid" toml:"enable-gtid" json:"enable-gtid"`
	// deprecated
	DisableCausality bool   `yaml:"disable-detect" toml:"disable-detect" json:"disable-detect"`
	SafeMode         bool   `yaml:"safe-mode" toml:"safe-mode" json:"safe-mode"`
	SafeModeDuration string `yaml:"safe-mode-duration" toml:"safe-mode-duration" json:"safe-mode-duration"`
	// deprecated, use `ansi-quotes` in top level config instead
	EnableANSIQuotes bool `yaml:"enable-ansi-quotes" toml:"enable-ansi-quotes" json:"enable-ansi-quotes"`
}

// DefaultSyncerConfig return default syncer config for task.
func DefaultSyncerConfig() SyncerConfig {
	return SyncerConfig{
		WorkerCount:             defaultWorkerCount,
		Batch:                   defaultBatch,
		QueueSize:               defaultQueueSize,
		CheckpointFlushInterval: defaultCheckpointFlushInterval,
		SafeModeDuration:        defaultSafeModeDuration,
	}
}

// alias to avoid infinite recursion for UnmarshalYAML.
type rawSyncerConfig SyncerConfig

// UnmarshalYAML implements Unmarshaler.UnmarshalYAML.
func (m *SyncerConfig) UnmarshalYAML(unmarshal func(interface{}) error) error {
	raw := rawSyncerConfig(DefaultSyncerConfig())
	if err := unmarshal(&raw); err != nil {
		return terror.ErrConfigYamlTransform.Delegate(err, "unmarshal syncer config")
	}
	*m = SyncerConfig(raw) // raw used only internal, so no deep copy
	return nil
}

type ValidatorConfig struct {
	Mode               string   `yaml:"mode" toml:"mode" json:"mode"`
	WorkerCount        int      `yaml:"worker-count" toml:"worker-count" json:"worker-count"`
	ValidateInterval   Duration `yaml:"validate-interval" toml:"validate-interval" json:"validate-interval"`
	CheckInterval      Duration `yaml:"check-interval" toml:"check-interval" json:"check-interval"`
	RowErrorDelay      Duration `yaml:"row-error-delay" toml:"row-error-delay" json:"row-error-delay"`
	MetaFlushInterval  Duration `yaml:"meta-flush-interval" toml:"meta-flush-interval" json:"meta-flush-interval"`
	BatchQuerySize     int      `yaml:"batch-query-size" toml:"batch-query-size" json:"batch-query-size"`
	MaxPendingRowSize  string   `yaml:"max-pending-row-size" toml:"max-pending-row-size" json:"max-pending-row-size"`
	MaxPendingRowCount int      `yaml:"max-pending-row-count" toml:"max-pending-row-count" json:"max-pending-row-count"`
	StartTime          string   `yaml:"-" toml:"start-time" json:"-"`
}

func (v *ValidatorConfig) Adjust() error {
	if v.Mode == "" {
		v.Mode = ValidationNone
	}
	if v.Mode != ValidationNone && v.Mode != ValidationFast && v.Mode != ValidationFull {
		return terror.ErrConfigValidationMode
	}
	if v.WorkerCount <= 0 {
		v.WorkerCount = DefaultValidatorWorkerCount
	}
	if v.ValidateInterval.Duration == 0 {
		v.ValidateInterval.Duration = DefaultValidatorValidateInterval
	}
	if v.CheckInterval.Duration == 0 {
		v.CheckInterval.Duration = DefaultValidatorCheckInterval
	}
	if v.RowErrorDelay.Duration == 0 {
		v.RowErrorDelay.Duration = DefaultValidatorRowErrorDelay
	}
	if v.MetaFlushInterval.Duration == 0 {
		v.MetaFlushInterval.Duration = DefaultValidatorMetaFlushInterval
	}
	if v.BatchQuerySize == 0 {
		v.BatchQuerySize = DefaultValidatorBatchQuerySize
	}
	if v.MaxPendingRowSize == "" {
		v.MaxPendingRowSize = DefaultValidatorMaxPendingRowSize
	}

	_, err := units.RAMInBytes(v.MaxPendingRowSize)
	if err != nil {
		return err
	}
	if v.MaxPendingRowCount == 0 {
		v.MaxPendingRowCount = DefaultValidatorMaxPendingRow
	}
	return nil
}

func defaultValidatorConfig() ValidatorConfig {
	return ValidatorConfig{
		Mode: ValidationNone,
	}
}

// TaskConfig is the configuration for Task.
type TaskConfig struct {
	*flag.FlagSet `yaml:"-" toml:"-" json:"-"`

	Name                      string `yaml:"name" toml:"name" json:"name"`
	TaskMode                  string `yaml:"task-mode" toml:"task-mode" json:"task-mode"`
	IsSharding                bool   `yaml:"is-sharding" toml:"is-sharding" json:"is-sharding"`
	ShardMode                 string `yaml:"shard-mode" toml:"shard-mode" json:"shard-mode"` // when `shard-mode` set, we always enable sharding support.
	StrictOptimisticShardMode bool   `yaml:"strict-optimistic-shard-mode" toml:"strict-optimistic-shard-mode" json:"strict-optimistic-shard-mode"`
	// treat it as hidden configuration
	IgnoreCheckingItems []string `yaml:"ignore-checking-items" toml:"ignore-checking-items" json:"ignore-checking-items"`
	// we store detail status in meta
	// don't save configuration into it
	MetaSchema string `yaml:"meta-schema" toml:"meta-schema" json:"meta-schema"`
	// deprecated
	EnableHeartbeat bool `yaml:"enable-heartbeat" toml:"enable-heartbeat" json:"enable-heartbeat"`
	// deprecated
	HeartbeatUpdateInterval int `yaml:"heartbeat-update-interval" toml:"heartbeat-update-interval" json:"heartbeat-update-interval"`
	// deprecated
	HeartbeatReportInterval int    `yaml:"heartbeat-report-interval" toml:"heartbeat-report-interval" json:"heartbeat-report-interval"`
	Timezone                string `yaml:"timezone" toml:"timezone" json:"timezone"`

	// handle schema/table name mode, and only for schema/table name
	// if case insensitive, we would convert schema/table name to lower case
	CaseSensitive bool `yaml:"case-sensitive" toml:"case-sensitive" json:"case-sensitive"`

	// default "loose" handle create sql by original sql, will not add default collation as upstream
	// "strict" will add default collation as upstream, and downstream will occur error when downstream don't support
	CollationCompatible string `yaml:"collation_compatible" toml:"collation_compatible" json:"collation_compatible"`

	TargetDB *dbconfig.DBConfig `yaml:"target-database" toml:"target-database" json:"target-database"`

	MySQLInstances []*MySQLInstance `yaml:"mysql-instances" toml:"mysql-instances" json:"mysql-instances"`

	OnlineDDL bool `yaml:"online-ddl" toml:"online-ddl" json:"online-ddl"`
	// pt/gh-ost name rule,support regex
	ShadowTableRules []string `yaml:"shadow-table-rules" toml:"shadow-table-rules" json:"shadow-table-rules"`
	TrashTableRules  []string `yaml:"trash-table-rules" toml:"trash-table-rules" json:"trash-table-rules"`

	// deprecated
	OnlineDDLScheme string `yaml:"online-ddl-scheme" toml:"online-ddl-scheme" json:"online-ddl-scheme"`

	Routes  map[string]*router.TableRule   `yaml:"routes" toml:"routes" json:"routes"`
	Filters map[string]*bf.BinlogEventRule `yaml:"filters" toml:"filters" json:"filters"`
	// deprecated
	ColumnMappings map[string]*column.Rule      `yaml:"column-mappings" toml:"column-mappings" json:"column-mappings"`
	ExprFilter     map[string]*ExpressionFilter `yaml:"expression-filter" toml:"expression-filter" json:"expression-filter"`

	// black-white-list is deprecated, use block-allow-list instead
	BWList map[string]*filter.Rules `yaml:"black-white-list" toml:"black-white-list" json:"black-white-list"`
	BAList map[string]*filter.Rules `yaml:"block-allow-list" toml:"block-allow-list" json:"block-allow-list"`

	Mydumpers  map[string]*MydumperConfig  `yaml:"mydumpers" toml:"mydumpers" json:"mydumpers"`
	Loaders    map[string]*LoaderConfig    `yaml:"loaders" toml:"loaders" json:"loaders"`
	Syncers    map[string]*SyncerConfig    `yaml:"syncers" toml:"syncers" json:"syncers"`
	Validators map[string]*ValidatorConfig `yaml:"validators" toml:"validators" json:"validators"`

	CleanDumpFile bool `yaml:"clean-dump-file" toml:"clean-dump-file" json:"clean-dump-file"`
	// deprecated
	EnableANSIQuotes bool `yaml:"ansi-quotes" toml:"ansi-quotes" json:"ansi-quotes"`

	// deprecated, replaced by `start-task --remove-meta`
	RemoveMeta bool `yaml:"remove-meta"`

	// task experimental configs
	Experimental struct {
		AsyncCheckpointFlush bool `yaml:"async-checkpoint-flush" toml:"async-checkpoint-flush" json:"async-checkpoint-flush"`
	} `yaml:"experimental" toml:"experimental" json:"experimental"`
}

// NewTaskConfig creates a TaskConfig.
func NewTaskConfig() *TaskConfig {
	cfg := &TaskConfig{
		// explicitly set default value
		MetaSchema:              defaultMetaSchema,
		EnableHeartbeat:         defaultEnableHeartbeat,
		HeartbeatUpdateInterval: defaultUpdateInterval,
		HeartbeatReportInterval: defaultReportInterval,
		MySQLInstances:          make([]*MySQLInstance, 0, 5),
		IsSharding:              defaultIsSharding,
		Routes:                  make(map[string]*router.TableRule),
		Filters:                 make(map[string]*bf.BinlogEventRule),
		ColumnMappings:          make(map[string]*column.Rule),
		ExprFilter:              make(map[string]*ExpressionFilter),
		BWList:                  make(map[string]*filter.Rules),
		BAList:                  make(map[string]*filter.Rules),
		Mydumpers:               make(map[string]*MydumperConfig),
		Loaders:                 make(map[string]*LoaderConfig),
		Syncers:                 make(map[string]*SyncerConfig),
		Validators:              make(map[string]*ValidatorConfig),
		CleanDumpFile:           true,
		OnlineDDL:               true,
		CollationCompatible:     defaultCollationCompatible,
	}
	cfg.FlagSet = flag.NewFlagSet("task", flag.ContinueOnError)
	return cfg
}

// String returns the config's yaml string.
func (c *TaskConfig) String() string {
	cfg, err := yaml.Marshal(c)
	if err != nil {
		log.L().Error("marshal task config to yaml", zap.String("task", c.Name), log.ShortError(err))
	}
	return string(cfg)
}

// JSON returns the config's json string.
func (c *TaskConfig) JSON() string {
	//nolint:staticcheck
	cfg, err := json.Marshal(c)
	if err != nil {
		log.L().Error("marshal task config to json", zap.String("task", c.Name), log.ShortError(err))
	}
	return string(cfg)
}

// DecodeFile loads and decodes config from file.
func (c *TaskConfig) DecodeFile(fpath string) error {
	bs, err := os.ReadFile(fpath)
	if err != nil {
		return terror.ErrConfigReadCfgFromFile.Delegate(err, fpath)
	}

	err = yaml.UnmarshalStrict(bs, c)
	if err != nil {
		return terror.ErrConfigYamlTransform.Delegate(err)
	}

	return c.adjust()
}

// FromYaml loads config from file data.
func (c *TaskConfig) FromYaml(data string) error {
	err := yaml.UnmarshalStrict([]byte(data), c)
	if err != nil {
		return terror.ErrConfigYamlTransform.Delegate(err, "decode task config failed")
	}

	return c.adjust()
}

// RawDecode loads config from file data.
func (c *TaskConfig) RawDecode(data string) error {
	return terror.ErrConfigYamlTransform.Delegate(yaml.UnmarshalStrict([]byte(data), c), "decode task config failed")
}

// find unused items in config.
var configRefPrefixes = []string{"RouteRules", "FilterRules", "Mydumper", "Loader", "Syncer", "ExprFilter", "Validator"}

const (
	routeRulesIdx = iota
	filterRulesIdx
	mydumperIdx
	loaderIdx
	syncerIdx
	exprFilterIdx
	validatorIdx
)

// Adjust adjusts and verifies config.
func (c *TaskConfig) Adjust() error {
	if c == nil {
		return terror.ErrConfigYamlTransform.New("task config is nil")
	}
	return c.adjust()
}

func (c *TaskConfig) adjust() error {
	if len(c.Name) == 0 {
		return terror.ErrConfigNeedUniqueTaskName.Generate()
	}
	switch c.TaskMode {
	case ModeFull, ModeIncrement, ModeAll, ModeDump, ModeLoadSync:
	default:
		return terror.ErrConfigInvalidTaskMode.Generate()
	}
	if c.MetaSchema == "" {
		c.MetaSchema = defaultMetaSchema
	}

	if c.ShardMode != "" && c.ShardMode != ShardPessimistic && c.ShardMode != ShardOptimistic {
		return terror.ErrConfigShardModeNotSupport.Generate(c.ShardMode)
	} else if c.ShardMode == "" && c.IsSharding {
		c.ShardMode = ShardPessimistic // use the pessimistic mode as default for back compatible.
	}
	if c.StrictOptimisticShardMode && c.ShardMode != ShardOptimistic {
		return terror.ErrConfigStrictOptimisticShardMode.Generate()
	}

	if len(c.ColumnMappings) > 0 {
		return terror.ErrConfigColumnMappingDeprecated.Generate()
	}

	if c.CollationCompatible != "" && c.CollationCompatible != LooseCollationCompatible && c.CollationCompatible != StrictCollationCompatible {
		return terror.ErrConfigCollationCompatibleNotSupport.Generate(c.CollationCompatible)
	} else if c.CollationCompatible == "" {
		c.CollationCompatible = LooseCollationCompatible
	}

	for _, item := range c.IgnoreCheckingItems {
		if err := ValidateCheckingItem(item); err != nil {
			return err
		}
	}

	if c.OnlineDDLScheme != "" && c.OnlineDDLScheme != PT && c.OnlineDDLScheme != GHOST {
		return terror.ErrConfigOnlineSchemeNotSupport.Generate(c.OnlineDDLScheme)
	} else if c.OnlineDDLScheme == PT || c.OnlineDDLScheme == GHOST {
		c.OnlineDDL = true
		log.L().Warn("'online-ddl-scheme' will be deprecated soon. Recommend that use online-ddl instead of online-ddl-scheme.")
	}

	if c.TargetDB == nil {
		return terror.ErrConfigNeedTargetDB.Generate()
	}

	if len(c.MySQLInstances) == 0 {
		return terror.ErrConfigMySQLInstsAtLeastOne.Generate()
	}

	for name, exprFilter := range c.ExprFilter {
		if exprFilter.Schema == "" {
			return terror.ErrConfigExprFilterEmptyName.Generate(name, "schema")
		}
		if exprFilter.Table == "" {
			return terror.ErrConfigExprFilterEmptyName.Generate(name, "table")
		}
		setFields := make([]string, 0, 1)
		if exprFilter.InsertValueExpr != "" {
			if err := checkValidExpr(exprFilter.InsertValueExpr); err != nil {
				return terror.ErrConfigExprFilterWrongGrammar.Generate(name, exprFilter.InsertValueExpr, err)
			}
			setFields = append(setFields, "insert: ["+exprFilter.InsertValueExpr+"]")
		}
		if exprFilter.UpdateOldValueExpr != "" || exprFilter.UpdateNewValueExpr != "" {
			if exprFilter.UpdateOldValueExpr != "" {
				if err := checkValidExpr(exprFilter.UpdateOldValueExpr); err != nil {
					return terror.ErrConfigExprFilterWrongGrammar.Generate(name, exprFilter.UpdateOldValueExpr, err)
				}
			}
			if exprFilter.UpdateNewValueExpr != "" {
				if err := checkValidExpr(exprFilter.UpdateNewValueExpr); err != nil {
					return terror.ErrConfigExprFilterWrongGrammar.Generate(name, exprFilter.UpdateNewValueExpr, err)
				}
			}
			setFields = append(setFields, "update (old value): ["+exprFilter.UpdateOldValueExpr+"] update (new value): ["+exprFilter.UpdateNewValueExpr+"]")
		}
		if exprFilter.DeleteValueExpr != "" {
			if err := checkValidExpr(exprFilter.DeleteValueExpr); err != nil {
				return terror.ErrConfigExprFilterWrongGrammar.Generate(name, exprFilter.DeleteValueExpr, err)
			}
			setFields = append(setFields, "delete: ["+exprFilter.DeleteValueExpr+"]")
		}
		if len(setFields) > 1 {
			return terror.ErrConfigExprFilterManyExpr.Generate(name, setFields)
		}
	}

	for _, validatorCfg := range c.Validators {
		if err := validatorCfg.Adjust(); err != nil {
			return err
		}
	}

	instanceIDs := make(map[string]int) // source-id -> instance-index
	globalConfigReferCount := map[string]int{}
	duplicateErrorStrings := make([]string, 0)
	for i, inst := range c.MySQLInstances {
		if err := inst.VerifyAndAdjust(); err != nil {
			return terror.Annotatef(err, "mysql-instance: %s", humanize.Ordinal(i))
		}
		if iid, ok := instanceIDs[inst.SourceID]; ok {
			return terror.ErrConfigMySQLInstSameSourceID.Generate(iid, i, inst.SourceID)
		}
		instanceIDs[inst.SourceID] = i

		switch c.TaskMode {
		case ModeFull, ModeAll, ModeDump:
			if inst.Meta != nil {
				log.L().Warn("metadata will not be used. for Full mode, incremental sync will never occur; for All mode, the meta dumped by MyDumper will be used", zap.Int("mysql instance", i), zap.String("task mode", c.TaskMode))
			}
		case ModeIncrement:
			if inst.Meta == nil {
				log.L().Warn("mysql-instance doesn't set meta for incremental mode, user should specify start_time to start task.", zap.String("sourceID", inst.SourceID))
			} else {
				err := inst.Meta.Verify()
				if err != nil {
					return terror.Annotatef(err, "mysql-instance: %d", i)
				}
			}
		}

		for _, name := range inst.RouteRules {
			if _, ok := c.Routes[name]; !ok {
				return terror.ErrConfigRouteRuleNotFound.Generate(i, name)
			}
			globalConfigReferCount[configRefPrefixes[routeRulesIdx]+name]++
		}
		for _, name := range inst.FilterRules {
			if _, ok := c.Filters[name]; !ok {
				return terror.ErrConfigFilterRuleNotFound.Generate(i, name)
			}
			globalConfigReferCount[configRefPrefixes[filterRulesIdx]+name]++
		}

		// only when BAList is empty use BWList
		if len(c.BAList) == 0 && len(c.BWList) != 0 {
			c.BAList = c.BWList
		}
		if _, ok := c.BAList[inst.BAListName]; len(inst.BAListName) > 0 && !ok {
			return terror.ErrConfigBAListNotFound.Generate(i, inst.BAListName)
		}

		if len(inst.MydumperConfigName) > 0 {
			rule, ok := c.Mydumpers[inst.MydumperConfigName]
			if !ok {
				return terror.ErrConfigMydumperCfgNotFound.Generate(i, inst.MydumperConfigName)
			}
			globalConfigReferCount[configRefPrefixes[mydumperIdx]+inst.MydumperConfigName]++
			if rule != nil {
				inst.Mydumper = new(MydumperConfig)
				*inst.Mydumper = *rule // ref mydumper config
			}
		}
		if inst.Mydumper == nil {
			if len(c.Mydumpers) != 0 {
				log.L().Warn("mysql instance don't refer mydumper's configuration with mydumper-config-name, the default configuration will be used", zap.String("mysql instance", inst.SourceID))
			}
			defaultCfg := DefaultMydumperConfig()
			inst.Mydumper = &defaultCfg
		} else if inst.Mydumper.ChunkFilesize == "" {
			// avoid too big dump file that can't sent concurrently
			inst.Mydumper.ChunkFilesize = defaultChunkFilesize
		}
		if inst.MydumperThread != 0 {
			inst.Mydumper.Threads = inst.MydumperThread
		}

		if HasDump(c.TaskMode) && len(inst.Mydumper.MydumperPath) == 0 {
			// only verify if set, whether is valid can only be verify when we run it
			return terror.ErrConfigMydumperPathNotValid.Generate(i)
		}

		if len(inst.LoaderConfigName) > 0 {
			rule, ok := c.Loaders[inst.LoaderConfigName]
			if !ok {
				return terror.ErrConfigLoaderCfgNotFound.Generate(i, inst.LoaderConfigName)
			}
			globalConfigReferCount[configRefPrefixes[loaderIdx]+inst.LoaderConfigName]++
			if rule != nil {
				inst.Loader = new(LoaderConfig)
				*inst.Loader = *rule // ref loader config
			}
		}
		if inst.Loader == nil {
			if len(c.Loaders) != 0 {
				log.L().Warn("mysql instance don't refer loader's configuration with loader-config-name, the default configuration will be used", zap.String("mysql instance", inst.SourceID))
			}
			defaultCfg := DefaultLoaderConfig()
			inst.Loader = &defaultCfg
		}
		if inst.LoaderThread != 0 {
			inst.Loader.PoolSize = inst.LoaderThread
		}

		if len(inst.SyncerConfigName) > 0 {
			rule, ok := c.Syncers[inst.SyncerConfigName]
			if !ok {
				return terror.ErrConfigSyncerCfgNotFound.Generate(i, inst.SyncerConfigName)
			}
			globalConfigReferCount[configRefPrefixes[syncerIdx]+inst.SyncerConfigName]++
			if rule != nil {
				inst.Syncer = new(SyncerConfig)
				*inst.Syncer = *rule // ref syncer config
			}
		}
		if inst.Syncer == nil {
			if len(c.Syncers) != 0 {
				log.L().Warn("mysql instance don't refer syncer's configuration with syncer-config-name, the default configuration will be used", zap.String("mysql instance", inst.SourceID))
			}
			defaultCfg := DefaultSyncerConfig()
			inst.Syncer = &defaultCfg
		}
		if inst.Syncer.QueueSize == 0 {
			inst.Syncer.QueueSize = defaultQueueSize
		}
		if inst.Syncer.CheckpointFlushInterval == 0 {
			inst.Syncer.CheckpointFlushInterval = defaultCheckpointFlushInterval
		}
		if inst.Syncer.SafeModeDuration == "" {
			inst.Syncer.SafeModeDuration = strconv.Itoa(2*inst.Syncer.CheckpointFlushInterval) + "s"
		}
		if duration, err := time.ParseDuration(inst.Syncer.SafeModeDuration); err != nil {
			return terror.ErrConfigInvalidSafeModeDuration.Generate(inst.Syncer.SafeModeDuration, err)
		} else if inst.Syncer.SafeMode && duration == 0 {
			return terror.ErrConfigConfictSafeModeDurationAndSafeMode.Generate()
		}
		if inst.SyncerThread != 0 {
			inst.Syncer.WorkerCount = inst.SyncerThread
		}

		inst.ContinuousValidator = defaultValidatorConfig()
		if inst.ContinuousValidatorConfigName != "" {
			rule, ok := c.Validators[inst.ContinuousValidatorConfigName]
			if !ok {
				return terror.ErrContinuousValidatorCfgNotFound.Generate(i, inst.ContinuousValidatorConfigName)
			}
			globalConfigReferCount[configRefPrefixes[validatorIdx]+inst.ContinuousValidatorConfigName]++
			if rule != nil {
				inst.ContinuousValidator = *rule
			}
		}

		// for backward compatible, set global config `ansi-quotes: true` if any syncer is true
		if inst.Syncer.EnableANSIQuotes {
			log.L().Warn("DM could discover proper ANSI_QUOTES, `enable-ansi-quotes` is no longer take effect")
		}
		if inst.Syncer.DisableCausality {
			log.L().Warn("`disable-causality` is no longer take effect")
		}

		for _, name := range inst.ExpressionFilters {
			if _, ok := c.ExprFilter[name]; !ok {
				return terror.ErrConfigExprFilterNotFound.Generate(i, name)
			}
			globalConfigReferCount[configRefPrefixes[exprFilterIdx]+name]++
		}

		if dupeRules := checkDuplicateString(inst.RouteRules); len(dupeRules) > 0 {
			duplicateErrorStrings = append(duplicateErrorStrings, fmt.Sprintf("mysql-instance(%d)'s route-rules: %s", i, strings.Join(dupeRules, ", ")))
		}
		if dupeRules := checkDuplicateString(inst.FilterRules); len(dupeRules) > 0 {
			duplicateErrorStrings = append(duplicateErrorStrings, fmt.Sprintf("mysql-instance(%d)'s filter-rules: %s", i, strings.Join(dupeRules, ", ")))
		}
		if dupeRules := checkDuplicateString(inst.ColumnMappingRules); len(dupeRules) > 0 {
			duplicateErrorStrings = append(duplicateErrorStrings, fmt.Sprintf("mysql-instance(%d)'s column-mapping-rules: %s", i, strings.Join(dupeRules, ", ")))
		}
		if dupeRules := checkDuplicateString(inst.ExpressionFilters); len(dupeRules) > 0 {
			duplicateErrorStrings = append(duplicateErrorStrings, fmt.Sprintf("mysql-instance(%d)'s expression-filters: %s", i, strings.Join(dupeRules, ", ")))
		}
	}
	if len(duplicateErrorStrings) > 0 {
		return terror.ErrConfigDuplicateCfgItem.Generate(strings.Join(duplicateErrorStrings, "\n"))
	}

	var unusedConfigs []string
	for route := range c.Routes {
		if globalConfigReferCount[configRefPrefixes[routeRulesIdx]+route] == 0 {
			unusedConfigs = append(unusedConfigs, route)
		}
	}
	for filter := range c.Filters {
		if globalConfigReferCount[configRefPrefixes[filterRulesIdx]+filter] == 0 {
			unusedConfigs = append(unusedConfigs, filter)
		}
	}
	for mydumper := range c.Mydumpers {
		if globalConfigReferCount[configRefPrefixes[mydumperIdx]+mydumper] == 0 {
			unusedConfigs = append(unusedConfigs, mydumper)
		}
	}

	for loader, cfg := range c.Loaders {
		if cfg != nil {
			if err1 := cfg.adjust(); err1 != nil {
				return err1
			}
		}
		if globalConfigReferCount[configRefPrefixes[loaderIdx]+loader] == 0 {
			unusedConfigs = append(unusedConfigs, loader)
		}
	}
	for syncer := range c.Syncers {
		if globalConfigReferCount[configRefPrefixes[syncerIdx]+syncer] == 0 {
			unusedConfigs = append(unusedConfigs, syncer)
		}
	}
	for exprFilter := range c.ExprFilter {
		if globalConfigReferCount[configRefPrefixes[exprFilterIdx]+exprFilter] == 0 {
			unusedConfigs = append(unusedConfigs, exprFilter)
		}
	}
	for key := range c.Validators {
		if globalConfigReferCount[configRefPrefixes[validatorIdx]+key] == 0 {
			unusedConfigs = append(unusedConfigs, key)
		}
	}

	if len(unusedConfigs) != 0 {
		sort.Strings(unusedConfigs)
		return terror.ErrConfigGlobalConfigsUnused.Generate(unusedConfigs)
	}

	// we postpone default time_zone init in each unit so we won't change the config value in task/sub_task config
	if c.Timezone != "" {
		if _, err := utils.ParseTimeZone(c.Timezone); err != nil {
			return err
		}
	}
	if c.RemoveMeta {
		log.L().Warn("`remove-meta` in task config is deprecated, please use `start-task ... --remove-meta` instead")
	}

	if c.EnableHeartbeat || c.HeartbeatUpdateInterval != defaultUpdateInterval ||
		c.HeartbeatReportInterval != defaultReportInterval {
		c.EnableHeartbeat = false
		log.L().Warn("heartbeat is deprecated, needn't set it anymore.")
	}
	return nil
}

// getGenerateName generates name by rule or gets name from nameMap
// if it's a new name, increase nameIdx
// otherwise return current nameIdx.
func getGenerateName(rule interface{}, nameIdx int, namePrefix string, nameMap map[string]string) (string, int) {
	// use json as key since no DeepEqual for rules now.
	ruleByte, err := json.Marshal(rule)
	if err != nil {
		log.L().Error(fmt.Sprintf("marshal %s rule to json", namePrefix), log.ShortError(err))
		return fmt.Sprintf("%s-%02d", namePrefix, nameIdx), nameIdx + 1
	} else if val, ok := nameMap[string(ruleByte)]; ok {
		return val, nameIdx
	} else {
		ruleName := fmt.Sprintf("%s-%02d", namePrefix, nameIdx+1)
		nameMap[string(ruleByte)] = ruleName
		return ruleName, nameIdx + 1
	}
}

// checkDuplicateString checks whether the given string array has duplicate string item
// if there is duplicate, it will return **all** the duplicate strings.
func checkDuplicateString(ruleNames []string) []string {
	mp := make(map[string]bool, len(ruleNames))
	dupeArray := make([]string, 0)
	for _, name := range ruleNames {
		if added, ok := mp[name]; ok {
			if !added {
				dupeArray = append(dupeArray, name)
				mp[name] = true
			}
		} else {
			mp[name] = false
		}
	}
	return dupeArray
}

// AdjustTargetDBSessionCfg adjust session cfg of TiDB.
func AdjustTargetDBSessionCfg(dbConfig *dbconfig.DBConfig, version *semver.Version) {
	lowerMap := make(map[string]string, len(dbConfig.Session))
	for k, v := range dbConfig.Session {
		lowerMap[strings.ToLower(k)] = v
	}
	// all cfg in defaultSessionCfg should be lower case
	for _, cfg := range defaultSessionCfg {
		if _, ok := lowerMap[cfg.key]; !ok && !version.LessThan(*cfg.minVersion) {
			lowerMap[cfg.key] = cfg.val
		}
	}
	dbConfig.Session = lowerMap
}

var (
	defaultParser = parser.New()
	parserMu      sync.Mutex
)

func checkValidExpr(expr string) error {
	expr = "select " + expr
	parserMu.Lock()
	_, _, err := defaultParser.Parse(expr, "", "")
	parserMu.Unlock()
	return err
}

// YamlForDowngrade returns YAML format represents of config for downgrade.
func (c *TaskConfig) YamlForDowngrade() (string, error) {
	t := NewTaskConfigForDowngrade(c)

	// try to encrypt password
	t.TargetDB.Password = utils.EncryptOrPlaintext(utils.DecryptOrPlaintext(t.TargetDB.Password))

	// omit default values, so we can ignore them for later marshal
	t.omitDefaultVals()

	return t.Yaml()
}

// MySQLInstanceForDowngrade represents a sync config of a MySQL instance for downgrade.
type MySQLInstanceForDowngrade struct {
	SourceID           string          `yaml:"source-id"`
	Meta               *Meta           `yaml:"meta"`
	FilterRules        []string        `yaml:"filter-rules"`
	ColumnMappingRules []string        `yaml:"column-mapping-rules"`
	RouteRules         []string        `yaml:"route-rules"`
	BWListName         string          `yaml:"black-white-list"`
	BAListName         string          `yaml:"block-allow-list"`
	MydumperConfigName string          `yaml:"mydumper-config-name"`
	Mydumper           *MydumperConfig `yaml:"mydumper"`
	MydumperThread     int             `yaml:"mydumper-thread"`
	LoaderConfigName   string          `yaml:"loader-config-name"`
	Loader             *LoaderConfig   `yaml:"loader"`
	LoaderThread       int             `yaml:"loader-thread"`
	SyncerConfigName   string          `yaml:"syncer-config-name"`
	Syncer             *SyncerConfig   `yaml:"syncer"`
	SyncerThread       int             `yaml:"syncer-thread"`
	// new config item
	ExpressionFilters []string `yaml:"expression-filters,omitempty"`
}

// NewMySQLInstancesForDowngrade creates []* MySQLInstanceForDowngrade.
func NewMySQLInstancesForDowngrade(mysqlInstances []*MySQLInstance) []*MySQLInstanceForDowngrade {
	mysqlInstancesForDowngrade := make([]*MySQLInstanceForDowngrade, 0, len(mysqlInstances))
	for _, m := range mysqlInstances {
		newMySQLInstance := &MySQLInstanceForDowngrade{
			SourceID:           m.SourceID,
			Meta:               m.Meta,
			FilterRules:        m.FilterRules,
			ColumnMappingRules: m.ColumnMappingRules,
			RouteRules:         m.RouteRules,
			BWListName:         m.BWListName,
			BAListName:         m.BAListName,
			MydumperConfigName: m.MydumperConfigName,
			Mydumper:           m.Mydumper,
			MydumperThread:     m.MydumperThread,
			LoaderConfigName:   m.LoaderConfigName,
			Loader:             m.Loader,
			LoaderThread:       m.LoaderThread,
			SyncerConfigName:   m.SyncerConfigName,
			Syncer:             m.Syncer,
			SyncerThread:       m.SyncerThread,
			ExpressionFilters:  m.ExpressionFilters,
		}
		mysqlInstancesForDowngrade = append(mysqlInstancesForDowngrade, newMySQLInstance)
	}
	return mysqlInstancesForDowngrade
}

// LoaderConfigForDowngrade is the base configuration for loader in v2.0.
// This config is used for downgrade(config export) from a higher dmctl version.
// When we add any new config item into LoaderConfig, we should update it also.
type LoaderConfigForDowngrade struct {
	PoolSize int    `yaml:"pool-size" toml:"pool-size" json:"pool-size"`
	Dir      string `yaml:"dir" toml:"dir" json:"dir"`
}

func NewLoaderConfigForDowngrade(loaderConfigs map[string]*LoaderConfig) map[string]*LoaderConfigForDowngrade {
	loaderConfigsForDowngrade := make(map[string]*LoaderConfigForDowngrade, len(loaderConfigs))
	for k, v := range loaderConfigs {
		loaderConfigsForDowngrade[k] = &LoaderConfigForDowngrade{
			PoolSize: v.PoolSize,
			Dir:      v.Dir,
		}
	}
	return loaderConfigsForDowngrade
}

// SyncerConfigForDowngrade is the base configuration for syncer in v2.0.
// This config is used for downgrade(config export) from a higher dmctl version.
// When we add any new config item into SyncerConfig, we should update it also.
type SyncerConfigForDowngrade struct {
	MetaFile                string `yaml:"meta-file"`
	WorkerCount             int    `yaml:"worker-count"`
	Batch                   int    `yaml:"batch"`
	QueueSize               int    `yaml:"queue-size"`
	CheckpointFlushInterval int    `yaml:"checkpoint-flush-interval"`
	MaxRetry                int    `yaml:"max-retry"`
	EnableGTID              bool   `yaml:"enable-gtid"`
	DisableCausality        bool   `yaml:"disable-detect"`
	SafeMode                bool   `yaml:"safe-mode"`
	EnableANSIQuotes        bool   `yaml:"enable-ansi-quotes"`

	SafeModeDuration string `yaml:"safe-mode-duration,omitempty"`
	Compact          bool   `yaml:"compact,omitempty"`
	MultipleRows     bool   `yaml:"multipleRows,omitempty"`
}

// NewSyncerConfigsForDowngrade converts SyncerConfig to SyncerConfigForDowngrade.
func NewSyncerConfigsForDowngrade(syncerConfigs map[string]*SyncerConfig) map[string]*SyncerConfigForDowngrade {
	syncerConfigsForDowngrade := make(map[string]*SyncerConfigForDowngrade, len(syncerConfigs))
	for configName, syncerConfig := range syncerConfigs {
		newSyncerConfig := &SyncerConfigForDowngrade{
			MetaFile:                syncerConfig.MetaFile,
			WorkerCount:             syncerConfig.WorkerCount,
			Batch:                   syncerConfig.Batch,
			QueueSize:               syncerConfig.QueueSize,
			CheckpointFlushInterval: syncerConfig.CheckpointFlushInterval,
			MaxRetry:                syncerConfig.MaxRetry,
			EnableGTID:              syncerConfig.EnableGTID,
			DisableCausality:        syncerConfig.DisableCausality,
			SafeMode:                syncerConfig.SafeMode,
			SafeModeDuration:        syncerConfig.SafeModeDuration,
			EnableANSIQuotes:        syncerConfig.EnableANSIQuotes,
			Compact:                 syncerConfig.Compact,
			MultipleRows:            syncerConfig.MultipleRows,
		}
		syncerConfigsForDowngrade[configName] = newSyncerConfig
	}
	return syncerConfigsForDowngrade
}

// omitDefaultVals change default value to empty value for new config item.
// If any default value for new config item is not empty(0 or false or nil),
// we should change it to empty.
func (c *SyncerConfigForDowngrade) omitDefaultVals() {
	if c.SafeModeDuration == strconv.Itoa(2*c.CheckpointFlushInterval)+"s" {
		c.SafeModeDuration = ""
	}
}

// TaskConfigForDowngrade is the base configuration for task in v2.0.
// This config is used for downgrade(config export) from a higher dmctl version.
// When we add any new config item into SourceConfig, we should update it also.
type TaskConfigForDowngrade struct {
	Name                    string                               `yaml:"name"`
	TaskMode                string                               `yaml:"task-mode"`
	IsSharding              bool                                 `yaml:"is-sharding"`
	ShardMode               string                               `yaml:"shard-mode"`
	IgnoreCheckingItems     []string                             `yaml:"ignore-checking-items"`
	MetaSchema              string                               `yaml:"meta-schema"`
	EnableHeartbeat         bool                                 `yaml:"enable-heartbeat"`
	HeartbeatUpdateInterval int                                  `yaml:"heartbeat-update-interval"`
	HeartbeatReportInterval int                                  `yaml:"heartbeat-report-interval"`
	Timezone                string                               `yaml:"timezone"`
	CaseSensitive           bool                                 `yaml:"case-sensitive"`
	TargetDB                *dbconfig.DBConfig                   `yaml:"target-database"`
	OnlineDDLScheme         string                               `yaml:"online-ddl-scheme"`
	Routes                  map[string]*router.TableRule         `yaml:"routes"`
	Filters                 map[string]*bf.BinlogEventRule       `yaml:"filters"`
	ColumnMappings          map[string]*column.Rule              `yaml:"column-mappings"`
	BWList                  map[string]*filter.Rules             `yaml:"black-white-list"`
	BAList                  map[string]*filter.Rules             `yaml:"block-allow-list"`
	Mydumpers               map[string]*MydumperConfig           `yaml:"mydumpers"`
	Loaders                 map[string]*LoaderConfigForDowngrade `yaml:"loaders"`
	Syncers                 map[string]*SyncerConfigForDowngrade `yaml:"syncers"`
	CleanDumpFile           bool                                 `yaml:"clean-dump-file"`
	EnableANSIQuotes        bool                                 `yaml:"ansi-quotes"`
	RemoveMeta              bool                                 `yaml:"remove-meta"`
	// new config item
	MySQLInstances            []*MySQLInstanceForDowngrade `yaml:"mysql-instances"`
	ExprFilter                map[string]*ExpressionFilter `yaml:"expression-filter,omitempty"`
	OnlineDDL                 bool                         `yaml:"online-ddl,omitempty"`
	ShadowTableRules          []string                     `yaml:"shadow-table-rules,omitempty"`
	TrashTableRules           []string                     `yaml:"trash-table-rules,omitempty"`
	StrictOptimisticShardMode bool                         `yaml:"strict-optimistic-shard-mode,omitempty"`
}

// NewTaskConfigForDowngrade create new TaskConfigForDowngrade.
func NewTaskConfigForDowngrade(taskConfig *TaskConfig) *TaskConfigForDowngrade {
	targetDB := *taskConfig.TargetDB
	return &TaskConfigForDowngrade{
		Name:                      taskConfig.Name,
		TaskMode:                  taskConfig.TaskMode,
		IsSharding:                taskConfig.IsSharding,
		ShardMode:                 taskConfig.ShardMode,
		StrictOptimisticShardMode: taskConfig.StrictOptimisticShardMode,
		IgnoreCheckingItems:       taskConfig.IgnoreCheckingItems,
		MetaSchema:                taskConfig.MetaSchema,
		EnableHeartbeat:           taskConfig.EnableHeartbeat,
		HeartbeatUpdateInterval:   taskConfig.HeartbeatUpdateInterval,
		HeartbeatReportInterval:   taskConfig.HeartbeatReportInterval,
		Timezone:                  taskConfig.Timezone,
		CaseSensitive:             taskConfig.CaseSensitive,
		TargetDB:                  &targetDB,
		OnlineDDLScheme:           taskConfig.OnlineDDLScheme,
		Routes:                    taskConfig.Routes,
		Filters:                   taskConfig.Filters,
		ColumnMappings:            taskConfig.ColumnMappings,
		BWList:                    taskConfig.BWList,
		BAList:                    taskConfig.BAList,
		Mydumpers:                 taskConfig.Mydumpers,
		Loaders:                   NewLoaderConfigForDowngrade(taskConfig.Loaders),
		Syncers:                   NewSyncerConfigsForDowngrade(taskConfig.Syncers),
		CleanDumpFile:             taskConfig.CleanDumpFile,
		EnableANSIQuotes:          taskConfig.EnableANSIQuotes,
		RemoveMeta:                taskConfig.RemoveMeta,
		MySQLInstances:            NewMySQLInstancesForDowngrade(taskConfig.MySQLInstances),
		ExprFilter:                taskConfig.ExprFilter,
		OnlineDDL:                 taskConfig.OnlineDDL,
		ShadowTableRules:          taskConfig.ShadowTableRules,
		TrashTableRules:           taskConfig.TrashTableRules,
	}
}

// omitDefaultVals change default value to empty value for new config item.
// If any default value for new config item is not empty(0 or false or nil),
// we should change it to empty.
func (c *TaskConfigForDowngrade) omitDefaultVals() {
	if len(c.ShadowTableRules) == 1 && c.ShadowTableRules[0] == DefaultShadowTableRules {
		c.ShadowTableRules = nil
	}
	if len(c.TrashTableRules) == 1 && c.TrashTableRules[0] == DefaultTrashTableRules {
		c.TrashTableRules = nil
	}
	for _, s := range c.Syncers {
		s.omitDefaultVals()
	}
	c.OnlineDDL = false
}

// Yaml returns YAML format representation of config.
func (c *TaskConfigForDowngrade) Yaml() (string, error) {
	b, err := yaml.Marshal(c)
	return string(b), err
}
