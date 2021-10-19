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
	"bytes"
	"context"
	"database/sql"
	_ "embed"
	"encoding/json"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/go-mysql-org/go-mysql/mysql"
	"gopkg.in/yaml.v2"

	bf "github.com/pingcap/tidb-tools/pkg/binlog-filter"

	"github.com/pingcap/dm/pkg/binlog"
	"github.com/pingcap/dm/pkg/gtid"
	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/dm/pkg/terror"
	"github.com/pingcap/dm/pkg/utils"
)

const (
	// the default base(min) server id generated by random.
	defaultBaseServerID = math.MaxUint32 / 10
	defaultRelayDir     = "relay-dir"
)

var getAllServerIDFunc = utils.GetAllServerID

// SampleConfigFile is sample config file of source.
// The embed source.yaml is a copy of dm/master/source.yaml, because embed
// can only match regular files in the current directory and subdirectories.
//go:embed source.yaml
var SampleConfigFile string

// PurgeConfig is the configuration for Purger.
type PurgeConfig struct {
	Interval    int64 `yaml:"interval" toml:"interval" json:"interval"`             // check whether need to purge at this @Interval (seconds)
	Expires     int64 `yaml:"expires" toml:"expires" json:"expires"`                // if file's modified time is older than @Expires (hours), then it can be purged
	RemainSpace int64 `yaml:"remain-space" toml:"remain-space" json:"remain-space"` // if remain space in @RelayBaseDir less than @RemainSpace (GB), then it can be purged
}

// SourceConfig is the configuration for source.
type SourceConfig struct {
	EnableGTID  bool   `yaml:"enable-gtid" toml:"enable-gtid" json:"enable-gtid"`
	AutoFixGTID bool   `yaml:"auto-fix-gtid" toml:"auto-fix-gtid" json:"auto-fix-gtid"`
	RelayDir    string `yaml:"relay-dir" toml:"relay-dir" json:"relay-dir"`
	MetaDir     string `yaml:"meta-dir" toml:"meta-dir" json:"meta-dir"`
	Flavor      string `yaml:"flavor" toml:"flavor" json:"flavor"`
	Charset     string `yaml:"charset" toml:"charset" json:"charset"`

	EnableRelay bool `yaml:"enable-relay" toml:"enable-relay" json:"enable-relay"`
	// relay synchronous starting point (if specified)
	RelayBinLogName string `yaml:"relay-binlog-name" toml:"relay-binlog-name" json:"relay-binlog-name"`
	RelayBinlogGTID string `yaml:"relay-binlog-gtid" toml:"relay-binlog-gtid" json:"relay-binlog-gtid"`
	// only use when worker bound source, do not marsh it
	UUIDSuffix int `yaml:"-" toml:"-" json:"-"`

	SourceID string   `yaml:"source-id" toml:"source-id" json:"source-id"`
	From     DBConfig `yaml:"from" toml:"from" json:"from"`

	// config items for purger
	Purge PurgeConfig `yaml:"purge" toml:"purge" json:"purge"`

	// config items for task status checker
	Checker CheckerConfig `yaml:"checker" toml:"checker" json:"checker"`

	// id of the worker on which this task run
	ServerID uint32 `yaml:"server-id" toml:"server-id" json:"server-id"`

	// deprecated tracer, to keep compatibility with older version
	Tracer map[string]interface{} `yaml:"tracer" toml:"tracer" json:"-"`

	CaseSensitive bool                  `yaml:"case-sensitive" toml:"case-sensitive" json:"case-sensitive"`
	Filters       []*bf.BinlogEventRule `yaml:"filters" toml:"filters" json:"filters"`
}

// NewSourceConfig creates a new base config for upstream MySQL/MariaDB source.
func NewSourceConfig() *SourceConfig {
	c := newSourceConfig()
	c.adjust()
	return c
}

// NewSourceConfig creates a new base config without adjust.
func newSourceConfig() *SourceConfig {
	c := &SourceConfig{
		Purge: PurgeConfig{
			Interval:    60 * 60,
			Expires:     0,
			RemainSpace: 15,
		},
		Checker: CheckerConfig{
			CheckEnable:     true,
			BackoffRollback: Duration{DefaultBackoffRollback},
			BackoffMax:      Duration{DefaultBackoffMax},
		},
	}
	return c
}

// Clone clones a config.
func (c *SourceConfig) Clone() *SourceConfig {
	clone := &SourceConfig{}
	*clone = *c
	return clone
}

// Toml returns TOML format representation of config.
func (c *SourceConfig) Toml() (string, error) {
	var b bytes.Buffer

	err := toml.NewEncoder(&b).Encode(c)
	if err != nil {
		log.L().Error("fail to marshal config to toml", log.ShortError(err))
	}

	return b.String(), nil
}

// Yaml returns YAML format representation of config.
func (c *SourceConfig) Yaml() (string, error) {
	b, err := yaml.Marshal(c)
	if err != nil {
		log.L().Error("fail to marshal config to yaml", log.ShortError(err))
	}

	return string(b), nil
}

// Parse parses flag definitions from the argument list.
// accept toml content for legacy use (mainly used by etcd).
func (c *SourceConfig) Parse(content string) error {
	// Parse first to get config file.
	metaData, err := toml.Decode(content, c)
	err2 := c.check(&metaData, err)
	if err2 != nil {
		return err2
	}
	return c.Verify()
}

// ParseYaml parses flag definitions from the argument list, content should be yaml format.
func ParseYaml(content string) (*SourceConfig, error) {
	c := newSourceConfig()
	if err := yaml.UnmarshalStrict([]byte(content), c); err != nil {
		return nil, terror.ErrConfigYamlTransform.Delegate(err, "decode source config")
	}
	c.adjust()
	return c, nil
}

// EncodeToml encodes config.
func (c *SourceConfig) EncodeToml() (string, error) {
	buf := new(bytes.Buffer)
	if err := toml.NewEncoder(buf).Encode(c); err != nil {
		return "", err
	}
	return buf.String(), nil
}

func (c *SourceConfig) String() string {
	cfg, err := json.Marshal(c)
	if err != nil {
		log.L().Error("fail to marshal config to json", log.ShortError(err))
	}
	return string(cfg)
}

func (c *SourceConfig) adjust() {
	c.From.Adjust()
	c.Checker.Adjust()
}

// Verify verifies the config.
func (c *SourceConfig) Verify() error {
	if len(c.SourceID) == 0 {
		return terror.ErrWorkerNeedSourceID.Generate()
	}
	if len(c.SourceID) > MaxSourceIDLength {
		return terror.ErrWorkerTooLongSourceID.Generate(c.SourceID, MaxSourceIDLength)
	}

	var err error
	if len(c.RelayBinLogName) > 0 {
		if !binlog.VerifyFilename(c.RelayBinLogName) {
			return terror.ErrWorkerRelayBinlogName.Generate(c.RelayBinLogName)
		}
	}
	if len(c.RelayBinlogGTID) > 0 {
		_, err = gtid.ParserGTID(c.Flavor, c.RelayBinlogGTID)
		if err != nil {
			return terror.WithClass(terror.Annotatef(err, "relay-binlog-gtid %s", c.RelayBinlogGTID), terror.ClassDMWorker)
		}
	}

	c.DecryptPassword()

	_, err = bf.NewBinlogEvent(c.CaseSensitive, c.Filters)
	if err != nil {
		return terror.ErrConfigBinlogEventFilter.Delegate(err)
	}

	if c.Checker.BackoffMax.Duration < c.Checker.BackoffMin.Duration {
		return terror.ErrConfigCheckerMaxTooSmall.Generate(c.Checker.BackoffMax.Duration, c.Checker.BackoffMin.Duration)
	}

	return nil
}

// DecryptPassword returns a decrypted config replica in config.
func (c *SourceConfig) DecryptPassword() *SourceConfig {
	clone := c.Clone()
	var pswdFrom string
	if len(clone.From.Password) > 0 {
		pswdFrom = utils.DecryptOrPlaintext(clone.From.Password)
	}
	clone.From.Password = pswdFrom
	return clone
}

// GenerateDBConfig creates DBConfig for DB.
func (c *SourceConfig) GenerateDBConfig() *DBConfig {
	// decrypt password
	clone := c.DecryptPassword()
	from := &clone.From
	from.RawDBCfg = DefaultRawDBConfig().SetReadTimeout(utils.DefaultDBTimeout.String())
	return from
}

// Adjust flavor and server-id of SourceConfig.
func (c *SourceConfig) Adjust(ctx context.Context, db *sql.DB) (err error) {
	c.From.Adjust()
	c.Checker.Adjust()

	// use one timeout for all following DB operations.
	ctx2, cancel := context.WithTimeout(ctx, utils.DefaultDBTimeout)
	defer cancel()
	if c.Flavor == "" || c.ServerID == 0 {
		err = c.AdjustFlavor(ctx2, db)
		if err != nil {
			return err
		}

		err = c.AdjustServerID(ctx2, db)
		if err != nil {
			return err
		}
	}

	// MariaDB automatically enabled gtid after 10.0.2, refer to https://mariadb.com/kb/en/gtid/#using-global-transaction-ids
	if c.EnableGTID && c.Flavor != mysql.MariaDBFlavor {
		val, err := utils.GetGTIDMode(ctx2, db)
		if err != nil {
			return err
		}
		if val != "ON" {
			return terror.ErrSourceCheckGTID.Generate(c.SourceID, val)
		}
	}

	if len(c.RelayDir) == 0 {
		c.RelayDir = defaultRelayDir
	}
	if filepath.IsAbs(c.RelayDir) {
		log.L().Warn("using an absolute relay path, relay log can't work when starting multiple relay worker")
	}

	return c.AdjustCaseSensitive(ctx2, db)
}

// AdjustCaseSensitive adjust CaseSensitive from DB.
func (c *SourceConfig) AdjustCaseSensitive(ctx context.Context, db *sql.DB) (err error) {
	caseSensitive, err2 := utils.GetDBCaseSensitive(ctx, db)
	if err2 != nil {
		return err2
	}
	c.CaseSensitive = caseSensitive
	return nil
}

// AdjustFlavor adjust Flavor from DB.
func (c *SourceConfig) AdjustFlavor(ctx context.Context, db *sql.DB) (err error) {
	if c.Flavor != "" {
		switch c.Flavor {
		case mysql.MariaDBFlavor, mysql.MySQLFlavor:
			return nil
		default:
			return terror.ErrNotSupportedFlavor.Generate(c.Flavor)
		}
	}

	c.Flavor, err = utils.GetFlavor(ctx, db)
	if ctx.Err() != nil {
		err = terror.Annotatef(err, "fail to get flavor info %v", ctx.Err())
	}
	return terror.WithScope(err, terror.ScopeUpstream)
}

// AdjustServerID adjust server id from DB.
func (c *SourceConfig) AdjustServerID(ctx context.Context, db *sql.DB) error {
	if c.ServerID != 0 {
		return nil
	}

	serverIDs, err := getAllServerIDFunc(ctx, db)
	if ctx.Err() != nil {
		err = terror.Annotatef(err, "fail to get server-id info %v", ctx.Err())
	}
	if err != nil {
		return terror.WithScope(err, terror.ScopeUpstream)
	}

	rand.Seed(time.Now().UnixNano())
	for i := 0; i < 5; i++ {
		randomValue := uint32(rand.Intn(100000))
		randomServerID := defaultBaseServerID + randomValue
		if _, ok := serverIDs[randomServerID]; ok {
			continue
		}

		c.ServerID = randomServerID
		return nil
	}

	return terror.ErrInvalidServerID.Generatef("can't find a random available server ID")
}

// LoadFromFile loads config from file.
func LoadFromFile(path string) (*SourceConfig, error) {
	c := newSourceConfig()
	content, err := os.ReadFile(path)
	if err != nil {
		return nil, terror.ErrConfigReadCfgFromFile.Delegate(err, path)
	}
	if err = yaml.UnmarshalStrict(content, c); err != nil {
		return nil, terror.ErrConfigYamlTransform.Delegate(err, "decode source config")
	}
	c.adjust()
	if err = c.Verify(); err != nil {
		return nil, err
	}
	return c, nil
}

func (c *SourceConfig) check(metaData *toml.MetaData, err error) error {
	if err != nil {
		return terror.ErrWorkerDecodeConfigFromFile.Delegate(err)
	}
	undecoded := metaData.Undecoded()
	if len(undecoded) > 0 && err == nil {
		var undecodedItems []string
		for _, item := range undecoded {
			undecodedItems = append(undecodedItems, item.String())
		}
		return terror.ErrWorkerUndecodedItemFromFile.Generate(strings.Join(undecodedItems, ","))
	}
	c.adjust()
	return nil
}

// YamlForDowngrade returns YAML format represents of config for downgrade.
func (c *SourceConfig) YamlForDowngrade() (string, error) {
	s := NewSourceConfigForDowngrade(c)

	// encrypt password
	cipher, err := utils.Encrypt(utils.DecryptOrPlaintext(c.From.Password))
	if err != nil {
		return "", err
	}
	s.From.Password = cipher

	// omit default values, so we can ignore them for later marshal
	s.omitDefaultVals()

	return s.Yaml()
}

// SourceConfigForDowngrade is the base configuration for source in v2.0.
// This config is used for downgrade(config export) from a higher dmctl version.
// When we add any new config item into SourceConfig, we should update it also.
type SourceConfigForDowngrade struct {
	EnableGTID      bool                   `yaml:"enable-gtid"`
	AutoFixGTID     bool                   `yaml:"auto-fix-gtid"`
	RelayDir        string                 `yaml:"relay-dir"`
	MetaDir         string                 `yaml:"meta-dir"`
	Flavor          string                 `yaml:"flavor"`
	Charset         string                 `yaml:"charset"`
	EnableRelay     bool                   `yaml:"enable-relay"`
	RelayBinLogName string                 `yaml:"relay-binlog-name"`
	RelayBinlogGTID string                 `yaml:"relay-binlog-gtid"`
	UUIDSuffix      int                    `yaml:"-"`
	SourceID        string                 `yaml:"source-id"`
	From            DBConfig               `yaml:"from"`
	Purge           PurgeConfig            `yaml:"purge"`
	Checker         CheckerConfig          `yaml:"checker"`
	ServerID        uint32                 `yaml:"server-id"`
	Tracer          map[string]interface{} `yaml:"tracer"`
	// any new config item, we mark it omitempty
	CaseSensitive bool                  `yaml:"case-sensitive,omitempty"`
	Filters       []*bf.BinlogEventRule `yaml:"filters,omitempty"`
}

// NewSourceConfigForDowngrade creates a new base config for downgrade.
func NewSourceConfigForDowngrade(sourceCfg *SourceConfig) *SourceConfigForDowngrade {
	return &SourceConfigForDowngrade{
		EnableGTID:      sourceCfg.EnableGTID,
		AutoFixGTID:     sourceCfg.AutoFixGTID,
		RelayDir:        sourceCfg.RelayDir,
		MetaDir:         sourceCfg.MetaDir,
		Flavor:          sourceCfg.Flavor,
		Charset:         sourceCfg.Charset,
		EnableRelay:     sourceCfg.EnableRelay,
		RelayBinLogName: sourceCfg.RelayBinLogName,
		RelayBinlogGTID: sourceCfg.RelayBinlogGTID,
		UUIDSuffix:      sourceCfg.UUIDSuffix,
		SourceID:        sourceCfg.SourceID,
		From:            sourceCfg.From,
		Purge:           sourceCfg.Purge,
		Checker:         sourceCfg.Checker,
		ServerID:        sourceCfg.ServerID,
		Tracer:          sourceCfg.Tracer,
		CaseSensitive:   sourceCfg.CaseSensitive,
		Filters:         sourceCfg.Filters,
	}
}

// omitDefaultVals change default value to empty value for new config item.
// If any default value for new config item is not empty(0 or false or nil),
// we should change it to empty.
func (c *SourceConfigForDowngrade) omitDefaultVals() {
	if len(c.From.Session) > 0 {
		if timeZone, ok := c.From.Session["time_zone"]; ok && timeZone == defaultTimeZone {
			delete(c.From.Session, "time_zone")
		}
	}
}

// Yaml returns YAML format representation of the config.
func (c *SourceConfigForDowngrade) Yaml() (string, error) {
	b, err := yaml.Marshal(c)
	return string(b), err
}
