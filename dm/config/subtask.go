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
	"bytes"
	"context"
	"database/sql"
	_ "embed"
	"encoding/json"
	"flag"
	"fmt"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/BurntSushi/toml"
	bf "github.com/pingcap/tidb-tools/pkg/binlog-filter"
	"github.com/pingcap/tidb-tools/pkg/column-mapping"
	extstorage "github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/pkg/util/dbutil"
	"github.com/pingcap/tidb/pkg/util/filter"
	regexprrouter "github.com/pingcap/tidb/pkg/util/regexpr-router"
	router "github.com/pingcap/tidb/pkg/util/table-router"
	"github.com/pingcap/tiflow/dm/config/dbconfig"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/dm/pkg/storage"
	"github.com/pingcap/tiflow/dm/pkg/terror"
	"github.com/pingcap/tiflow/dm/pkg/utils"
	"github.com/pingcap/tiflow/engine/pkg/promutil"
	"github.com/pingcap/tiflow/pkg/version"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

// task modes.
const (
	ModeAll       = "all"
	ModeFull      = "full"
	ModeIncrement = "incremental"
	ModeDump      = "dump"
	ModeLoadSync  = "load&sync"

	DefaultShadowTableRules = "^_(.+)_(?:new|gho)$"
	DefaultTrashTableRules  = "^_(.+)_(?:ghc|del|old)$"

	ShadowTableRules              = "shadow-table-rules"
	TrashTableRules               = "trash-table-rules"
	TiDBLightningCheckpointPrefix = "tidb_lightning_checkpoint_"
)

// FetchTimeZoneSetting fetch target db global time_zone setting.
// TODO: move GetTimeZoneOffset and FormatTimeZoneOffset from TiDB to tiflow.
func FetchTimeZoneSetting(ctx context.Context, db *sql.DB) (string, error) {
	dur, err := dbutil.GetTimeZoneOffset(ctx, db)
	if err != nil {
		return "", err
	}
	return dbutil.FormatTimeZoneOffset(dur), nil
}

// GetDBConfigForTest is a helper function to get db config for unit test .
func GetDBConfigForTest() dbconfig.DBConfig {
	return dbconfig.DBConfig{Host: "localhost", User: "root", Password: "not a real password", Port: 3306}
}

// SubTaskConfig is the configuration for SubTask.
type SubTaskConfig struct {
	// BurntSushi/toml seems have a bug for flag "-"
	// when doing encoding, if we use `toml:"-"`, it still try to encode it
	// and it will panic because of unsupported type (reflect.Func)
	// so we should not export flagSet
	flagSet *flag.FlagSet

	// when in sharding, multi dm-workers do one task
	IsSharding                bool   `toml:"is-sharding" json:"is-sharding"`
	ShardMode                 string `toml:"shard-mode" json:"shard-mode"`
	StrictOptimisticShardMode bool   `toml:"strict-optimistic-shard-mode" json:"strict-optimistic-shard-mode"`
	OnlineDDL                 bool   `toml:"online-ddl" json:"online-ddl"`

	// pt/gh-ost name rule, support regex
	ShadowTableRules []string `yaml:"shadow-table-rules" toml:"shadow-table-rules" json:"shadow-table-rules"`
	TrashTableRules  []string `yaml:"trash-table-rules" toml:"trash-table-rules" json:"trash-table-rules"`

	// deprecated
	OnlineDDLScheme string `toml:"online-ddl-scheme" json:"online-ddl-scheme"`

	// handle schema/table name mode, and only for schema/table name/pattern
	// if case insensitive, we would convert schema/table name/pattern to lower case
	CaseSensitive bool `toml:"case-sensitive" json:"case-sensitive"`

	// default "loose" handle create sql by original sql, will not add default collation as upstream
	// "strict" will add default collation as upstream, and downstream will occur error when downstream don't support
	CollationCompatible string `yaml:"collation_compatible" toml:"collation_compatible" json:"collation_compatible"`

	Name string `toml:"name" json:"name"`
	Mode string `toml:"mode" json:"mode"`
	//  treat it as hidden configuration
	IgnoreCheckingItems []string `toml:"ignore-checking-items" json:"ignore-checking-items"`
	// it represents a MySQL/MariaDB instance or a replica group
	SourceID   string `toml:"source-id" json:"source-id"`
	ServerID   uint32 `toml:"server-id" json:"server-id"`
	Flavor     string `toml:"flavor" json:"flavor"`
	MetaSchema string `toml:"meta-schema" json:"meta-schema"`
	// deprecated
	HeartbeatUpdateInterval int `toml:"heartbeat-update-interval" json:"heartbeat-update-interval"`
	// deprecated
	HeartbeatReportInterval int `toml:"heartbeat-report-interval" json:"heartbeat-report-interval"`
	// deprecated
	EnableHeartbeat bool   `toml:"enable-heartbeat" json:"enable-heartbeat"`
	Timezone        string `toml:"timezone" json:"timezone"`

	Meta *Meta `toml:"meta" json:"meta"`

	// RelayDir get value from dm-worker config
	RelayDir string `toml:"relay-dir" json:"relay-dir"`

	// UseRelay get value from dm-worker's relayEnabled
	UseRelay bool              `toml:"use-relay" json:"use-relay"`
	From     dbconfig.DBConfig `toml:"from" json:"from"`
	To       dbconfig.DBConfig `toml:"to" json:"to"`

	RouteRules  []*router.TableRule   `toml:"route-rules" json:"route-rules"`
	FilterRules []*bf.BinlogEventRule `toml:"filter-rules" json:"filter-rules"`
	// deprecated
	ColumnMappingRules []*column.Rule      `toml:"mapping-rule" json:"mapping-rule"`
	ExprFilter         []*ExpressionFilter `yaml:"expression-filter" toml:"expression-filter" json:"expression-filter"`

	// black-white-list is deprecated, use block-allow-list instead
	BWList *filter.Rules `toml:"black-white-list" json:"black-white-list"`
	BAList *filter.Rules `toml:"block-allow-list" json:"block-allow-list"`

	MydumperConfig // Mydumper configuration
	LoaderConfig   // Loader configuration
	SyncerConfig   // Syncer configuration
	ValidatorCfg   ValidatorConfig

	// compatible with standalone dm unit
	LogLevel  string `toml:"log-level" json:"log-level"`
	LogFile   string `toml:"log-file" json:"log-file"`
	LogFormat string `toml:"log-format" json:"log-format"`
	LogRotate string `toml:"log-rotate" json:"log-rotate"`

	PprofAddr  string `toml:"pprof-addr" json:"pprof-addr"`
	StatusAddr string `toml:"status-addr" json:"status-addr"`

	ConfigFile string `toml:"-" json:"config-file"`

	CleanDumpFile bool `toml:"clean-dump-file" json:"clean-dump-file"`

	// deprecated, will auto discover SQL mode
	EnableANSIQuotes bool `toml:"ansi-quotes" json:"ansi-quotes"`

	// still needed by Syncer / Loader bin
	printVersion bool

	// which DM worker is running the subtask, this will be injected when the real worker starts running the subtask(StartSubTask).
	WorkerName string `toml:"-" json:"-"`
	// task experimental configs
	Experimental struct {
		AsyncCheckpointFlush bool `yaml:"async-checkpoint-flush" toml:"async-checkpoint-flush" json:"async-checkpoint-flush"`
	} `yaml:"experimental" toml:"experimental" json:"experimental"`

	// members below are injected by dataflow engine
	ExtStorage      extstorage.ExternalStorage `toml:"-" json:"-"`
	MetricsFactory  promutil.Factory           `toml:"-" json:"-"`
	FrameworkLogger *zap.Logger                `toml:"-" json:"-"`
	// members below are injected by dataflow engine, UUID should be unique in
	// one go runtime.
	// IOTotalBytes is used build TCPConnWithIOCounter and UUID is used to as a
	// key to let MySQL driver to find the right TCPConnWithIOCounter.
	UUID         string         `toml:"-" json:"-"`
	IOTotalBytes *atomic.Uint64 `toml:"-" json:"-"`

	// meter network usage from upstream
	// e.g., pulling binlog
	DumpUUID         string         `toml:"-" json:"-"`
	DumpIOTotalBytes *atomic.Uint64 `toml:"-" json:"-"`
}

// SampleSubtaskConfig is the content of subtask.toml in current folder.
//
//go:embed subtask.toml
var SampleSubtaskConfig string

// NewSubTaskConfig creates a new SubTaskConfig.
func NewSubTaskConfig() *SubTaskConfig {
	cfg := &SubTaskConfig{}
	return cfg
}

// GetFlagSet provides the pointer of subtask's flag set.
func (c *SubTaskConfig) GetFlagSet() *flag.FlagSet {
	return c.flagSet
}

// SetFlagSet writes back the flag set.
func (c *SubTaskConfig) SetFlagSet(flagSet *flag.FlagSet) {
	c.flagSet = flagSet
}

// String returns the config's json string.
func (c *SubTaskConfig) String() string {
	cfg, err := json.Marshal(c)
	if err != nil {
		log.L().Error("marshal subtask config to json", zap.String("task", c.Name), log.ShortError(err))
	}
	return string(cfg)
}

// Toml returns TOML format representation of config.
func (c *SubTaskConfig) Toml() (string, error) {
	var b bytes.Buffer
	enc := toml.NewEncoder(&b)
	if err := enc.Encode(c); err != nil {
		return "", terror.ErrConfigTomlTransform.Delegate(err, "encode subtask config")
	}
	return b.String(), nil
}

// DecodeFile loads and decodes config from file.
func (c *SubTaskConfig) DecodeFile(fpath string, verifyDecryptPassword bool) error {
	_, err := toml.DecodeFile(fpath, c)
	if err != nil {
		return terror.ErrConfigTomlTransform.Delegate(err, "decode subtask config from file")
	}

	return c.Adjust(verifyDecryptPassword)
}

// Decode loads config from file data.
func (c *SubTaskConfig) Decode(data string, verifyDecryptPassword bool) error {
	if _, err := toml.Decode(data, c); err != nil {
		return terror.ErrConfigTomlTransform.Delegate(err, "decode subtask config from data")
	}

	return c.Adjust(verifyDecryptPassword)
}

func adjustOnlineTableRules(ruleType string, rules []string) ([]string, error) {
	adjustedRules := make([]string, 0, len(rules))
	for _, r := range rules {
		if !strings.HasPrefix(r, "^") {
			r = "^" + r
		}

		if !strings.HasSuffix(r, "$") {
			r += "$"
		}

		p, err := regexp.Compile(r)
		if err != nil {
			return rules, terror.ErrConfigOnlineDDLInvalidRegex.Generate(ruleType, r, "fail to compile: "+err.Error())
		}
		if p.NumSubexp() != 1 {
			return rules, terror.ErrConfigOnlineDDLInvalidRegex.Generate(ruleType, r, "rule isn't contains exactly one submatch")
		}
		adjustedRules = append(adjustedRules, r)
	}
	return adjustedRules, nil
}

// Adjust adjusts and verifies configs.
func (c *SubTaskConfig) Adjust(verifyDecryptPassword bool) error {
	if c.Name == "" {
		return terror.ErrConfigTaskNameEmpty.Generate()
	}

	if c.SourceID == "" {
		return terror.ErrConfigEmptySourceID.Generate()
	}
	if len(c.SourceID) > MaxSourceIDLength {
		return terror.ErrConfigTooLongSourceID.Generate()
	}

	if c.ShardMode != "" && c.ShardMode != ShardPessimistic && c.ShardMode != ShardOptimistic {
		return terror.ErrConfigShardModeNotSupport.Generate(c.ShardMode)
	} else if c.ShardMode == "" && c.IsSharding {
		c.ShardMode = ShardPessimistic // use the pessimistic mode as default for back compatible.
	}
	if c.StrictOptimisticShardMode && c.ShardMode != ShardOptimistic {
		return terror.ErrConfigStrictOptimisticShardMode.Generate()
	}

	if len(c.ColumnMappingRules) > 0 {
		return terror.ErrConfigColumnMappingDeprecated.Generate()
	}

	if c.OnlineDDLScheme != "" && c.OnlineDDLScheme != PT && c.OnlineDDLScheme != GHOST {
		return terror.ErrConfigOnlineSchemeNotSupport.Generate(c.OnlineDDLScheme)
	} else if c.OnlineDDLScheme == PT || c.OnlineDDLScheme == GHOST {
		c.OnlineDDL = true
		log.L().Warn("'online-ddl-scheme' will be deprecated soon. Recommend that use online-ddl instead of online-ddl-scheme.")
	}
	if len(c.ShadowTableRules) == 0 {
		c.ShadowTableRules = []string{DefaultShadowTableRules}
	} else {
		shadowTableRule, err := adjustOnlineTableRules(ShadowTableRules, c.ShadowTableRules)
		if err != nil {
			return err
		}
		c.ShadowTableRules = shadowTableRule
	}

	if len(c.TrashTableRules) == 0 {
		c.TrashTableRules = []string{DefaultTrashTableRules}
	} else {
		trashTableRule, err := adjustOnlineTableRules(TrashTableRules, c.TrashTableRules)
		if err != nil {
			return err
		}
		c.TrashTableRules = trashTableRule
	}

	if c.MetaSchema == "" {
		c.MetaSchema = defaultMetaSchema
	}

	// adjust dir, no need to do for load&sync mode because it needs its own s3 repository
	if HasLoad(c.Mode) && c.Mode != ModeLoadSync {
		// check
		isS3 := storage.IsS3Path(c.LoaderConfig.Dir)
		if isS3 && c.ImportMode == LoadModeLoader {
			return terror.ErrConfigLoaderS3NotSupport.Generate(c.LoaderConfig.Dir)
		}
		// add suffix
		var dirSuffix string
		if isS3 {
			// we will dump files to s3 dir's subdirectory
			dirSuffix = "/" + c.Name + "." + c.SourceID
		} else {
			// TODO we will dump local file to dir's subdirectory, but it may have risk of compatibility, we will fix in other pr
			dirSuffix = "." + c.Name
		}
		newDir, err := storage.AdjustPath(c.LoaderConfig.Dir, dirSuffix)
		if err != nil {
			return terror.ErrConfigLoaderDirInvalid.Delegate(err, c.LoaderConfig.Dir)
		}
		c.LoaderConfig.Dir = newDir
	}

	// adjust sorting dir
	if HasLoad(c.Mode) {
		newDir := c.LoaderConfig.Dir
		if c.LoaderConfig.SortingDirPhysical == "" {
			if storage.IsLocalDiskPath(newDir) {
				// lightning will not recursively create directories, so we use same level dir
				c.LoaderConfig.SortingDirPhysical = newDir + ".sorting"
			} else {
				c.LoaderConfig.SortingDirPhysical = "./sorting." + url.PathEscape(c.Name)
			}
		}
	}

	if c.SyncerConfig.QueueSize == 0 {
		c.SyncerConfig.QueueSize = defaultQueueSize
	}
	if c.SyncerConfig.CheckpointFlushInterval == 0 {
		c.SyncerConfig.CheckpointFlushInterval = defaultCheckpointFlushInterval
	}
	if c.SyncerConfig.SafeModeDuration == "" {
		c.SyncerConfig.SafeModeDuration = strconv.Itoa(2*c.SyncerConfig.CheckpointFlushInterval) + "s"
	}
	if duration, err := time.ParseDuration(c.SyncerConfig.SafeModeDuration); err != nil {
		return terror.ErrConfigInvalidSafeModeDuration.Generate(c.SyncerConfig.SafeModeDuration, err)
	} else if c.SyncerConfig.SafeMode && duration == 0 {
		return terror.ErrConfigConfictSafeModeDurationAndSafeMode.Generate()
	}

	c.From.AdjustWithTimeZone(c.Timezone)
	c.To.AdjustWithTimeZone(c.Timezone)

	if verifyDecryptPassword {
		_, err1 := c.DecryptedClone()
		if err1 != nil {
			return err1
		}
	}

	// only when block-allow-list is nil use black-white-list
	if c.BAList == nil && c.BWList != nil {
		c.BAList = c.BWList
	}

	if _, err := filter.New(c.CaseSensitive, c.BAList); err != nil {
		return terror.ErrConfigGenBAList.Delegate(err)
	}
	if _, err := regexprrouter.NewRegExprRouter(c.CaseSensitive, c.RouteRules); err != nil {
		return terror.ErrConfigGenTableRouter.Delegate(err)
	}
	// NewMapping will fill arguments with the default values.
	if _, err := column.NewMapping(c.CaseSensitive, c.ColumnMappingRules); err != nil {
		return terror.ErrConfigGenColumnMapping.Delegate(err)
	}
	if _, err := utils.ParseFileSize(c.MydumperConfig.ChunkFilesize, 0); err != nil {
		return terror.ErrConfigInvalidChunkFileSize.Generate(c.MydumperConfig.ChunkFilesize)
	}

	if _, err := bf.NewBinlogEvent(c.CaseSensitive, c.FilterRules); err != nil {
		return terror.ErrConfigBinlogEventFilter.Delegate(err)
	}
	if err := c.LoaderConfig.adjust(); err != nil {
		return err
	}
	if err := c.ValidatorCfg.Adjust(); err != nil {
		return err
	}

	// TODO: check every member
	// TODO: since we checked here, we could remove other terror like ErrSyncerUnitGenBAList
	// TODO: or we should check at task config and source config rather than this subtask config, to reduce duplication

	return nil
}

// Parse parses flag definitions from the argument list.
func (c *SubTaskConfig) Parse(arguments []string, verifyDecryptPassword bool) error {
	// Parse first to get config file.
	err := c.flagSet.Parse(arguments)
	if err != nil {
		return terror.ErrConfigParseFlagSet.Delegate(err)
	}

	if c.printVersion {
		fmt.Println(version.GetRawInfo())
		return flag.ErrHelp
	}

	// Load config file if specified.
	if c.ConfigFile != "" {
		err = c.DecodeFile(c.ConfigFile, verifyDecryptPassword)
		if err != nil {
			return err
		}
	}

	// Parse again to replace with command line options.
	err = c.flagSet.Parse(arguments)
	if err != nil {
		return terror.ErrConfigParseFlagSet.Delegate(err)
	}

	if len(c.flagSet.Args()) != 0 {
		return terror.ErrConfigParseFlagSet.Generatef("'%s' is an invalid flag", c.flagSet.Arg(0))
	}

	return c.Adjust(verifyDecryptPassword)
}

// DecryptedClone tries to decrypt db password in config.
func (c *SubTaskConfig) DecryptedClone() (*SubTaskConfig, error) {
	clone, err := c.Clone()
	if err != nil {
		return nil, err
	}

	var (
		pswdTo   string
		pswdFrom string
	)
	if len(clone.To.Password) > 0 {
		pswdTo = utils.DecryptOrPlaintext(clone.To.Password)
	}
	if len(clone.From.Password) > 0 {
		pswdFrom = utils.DecryptOrPlaintext(clone.From.Password)
	}
	clone.From.Password = pswdFrom
	clone.To.Password = pswdTo

	return clone, nil
}

// Clone returns a replica of SubTaskConfig.
func (c *SubTaskConfig) Clone() (*SubTaskConfig, error) {
	content, err := c.Toml()
	if err != nil {
		return nil, err
	}

	clone := &SubTaskConfig{}
	_, err = toml.Decode(content, clone)
	if err != nil {
		return nil, terror.ErrConfigTomlTransform.Delegate(err, "decode subtask config from data")
	}

	return clone, nil
}
