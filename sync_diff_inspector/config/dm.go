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
	"crypto/aes"
	"crypto/cipher"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/BurntSushi/toml"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/util/filter"
	router "github.com/pingcap/tidb/pkg/util/table-router"
	"github.com/pingcap/tiflow/dm/config/security"
	"github.com/pingcap/tiflow/dm/pb"
	"github.com/pingcap/tiflow/dm/pkg/terror"
	"github.com/pingcap/tiflow/pkg/column-mapping"
	"go.uber.org/zap"
)

const (
	// dm's http api version, define in https://github.com/pingcap/dm/blob/master/dm/proto/dmmaster.proto
	apiVersion = "v1alpha1"
)

func getDMTaskCfgURL(dmAddr, task string) string {
	return fmt.Sprintf("%s/apis/%s/subtasks/%s", dmAddr, apiVersion, task)
}

// getDMTaskCfg gets dm's sub task config
func getDMTaskCfg(dmAddr, task string) ([]*SubTaskConfig, error) {
	tr := &http.Transport{
		// TODO: support tls
		// TLSClientConfig: tlsCfg,
	}
	client := &http.Client{Transport: tr}
	req, err := http.NewRequestWithContext(context.Background(), "GET", getDMTaskCfgURL(dmAddr, task), nil)
	if err != nil {
		return nil, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	getSubTaskCfgResp := &pb.GetSubTaskCfgResponse{}
	err = json.Unmarshal(body, getSubTaskCfgResp)
	if err != nil {
		return nil, err
	}

	if !getSubTaskCfgResp.Result {
		return nil, errors.Errorf("fail to get sub task config from DM, %s", getSubTaskCfgResp.Msg)
	}

	subTaskCfgs := make([]*SubTaskConfig, 0, len(getSubTaskCfgResp.Cfgs))
	for _, cfgBytes := range getSubTaskCfgResp.Cfgs {
		subtaskCfg := &SubTaskConfig{}
		err = subtaskCfg.Decode(cfgBytes, false)
		if err != nil {
			return nil, err
		}
		subtaskCfg.To.Password = DecryptOrPlaintext(subtaskCfg.To.Password)
		subtaskCfg.From.Password = DecryptOrPlaintext(subtaskCfg.From.Password)
		subTaskCfgs = append(subTaskCfgs, subtaskCfg)
	}

	log.Info("dm sub task configs", zap.Reflect("cfgs", subTaskCfgs))
	return subTaskCfgs, nil
}

// SubTaskConfig is the configuration for SubTask.
type SubTaskConfig struct {
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

	// RelayDir get value from dm-worker config
	RelayDir string `toml:"relay-dir" json:"relay-dir"`

	// UseRelay get value from dm-worker's relayEnabled
	UseRelay bool     `toml:"use-relay" json:"use-relay"`
	From     DBConfig `toml:"from" json:"from"`
	To       DBConfig `toml:"to" json:"to"`

	RouteRules []*router.TableRule `toml:"route-rules" json:"route-rules"`
	// FilterRules []*bf.BinlogEventRule `toml:"filter-rules" json:"filter-rules"`
	// deprecated
	ColumnMappingRules []*column.Rule `toml:"mapping-rule" json:"mapping-rule"`
	// ExprFilter         []*ExpressionFilter `yaml:"expression-filter" toml:"expression-filter" json:"expression-filter"`

	// black-white-list is deprecated, use block-allow-list instead
	BWList *filter.Rules `toml:"black-white-list" json:"black-white-list"`
	BAList *filter.Rules `toml:"block-allow-list" json:"block-allow-list"`

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

	// which DM worker is running the subtask, this will be injected when the real worker starts running the subtask(StartSubTask).
	WorkerName string `toml:"-" json:"-"`
	// task experimental configs
	Experimental struct {
		AsyncCheckpointFlush bool `yaml:"async-checkpoint-flush" toml:"async-checkpoint-flush" json:"async-checkpoint-flush"`
	} `yaml:"experimental" toml:"experimental" json:"experimental"`
}

// DBConfig is the DB configuration.
type DBConfig struct {
	Host     string `toml:"host" json:"host" yaml:"host"`
	Port     int    `toml:"port" json:"port" yaml:"port"`
	User     string `toml:"user" json:"user" yaml:"user"`
	Password string `toml:"password" json:"-" yaml:"password"` // omit it for privacy
	// deprecated, mysql driver could automatically fetch this value
	MaxAllowedPacket *int              `toml:"max-allowed-packet" json:"max-allowed-packet" yaml:"max-allowed-packet"`
	Session          map[string]string `toml:"session" json:"session" yaml:"session"`

	// security config
	Security *security.Security `toml:"security" json:"security" yaml:"security"`

	// RawDBCfg *RawDBConfig `toml:"-" json:"-" yaml:"-"`
	// Net      string       `toml:"-" json:"-" yaml:"-"`
}

// Decode loads config from file data.
func (c *SubTaskConfig) Decode(data string, verifyDecryptPassword bool) error {
	if _, err := toml.Decode(data, c); err != nil {
		return errors.New("decode subtask config from data")
	}

	return nil
}

// DecryptOrPlaintext tries to decrypt base64 encoded ciphertext to plaintext or return plaintext.
func DecryptOrPlaintext(ciphertextB64 string) string {
	plaintext, err := Decrypt(ciphertextB64)
	if err != nil {
		return ciphertextB64
	}
	return plaintext
}

// Decrypt tries to decrypt base64 encoded ciphertext to plaintext.
func Decrypt(ciphertextB64 string) (string, error) {
	ciphertext, err := base64.StdEncoding.DecodeString(ciphertextB64)
	if err != nil {
		return "", err
	}

	plaintext, err := decrypt(ciphertext)
	if err != nil {
		return "", err
	}
	return string(plaintext), nil
}

var (
	secretKey, _ = hex.DecodeString("a529b7665997f043a30ac8fadcb51d6aa032c226ab5b7750530b12b8c1a16a48")
	ivSep        = []byte("@") // ciphertext format: iv + ivSep + encrypted-plaintext
)

// decrypt decrypts ciphertext to plaintext.
func decrypt(ciphertext []byte) ([]byte, error) {
	block, err := aes.NewCipher(secretKey)
	if err != nil {
		return nil, err
	}

	if len(ciphertext) < block.BlockSize()+len(ivSep) {
		return nil, terror.ErrCiphertextLenNotValid.Generate(block.BlockSize()+len(ivSep), len(ciphertext))
	}

	if !bytes.Equal(ciphertext[block.BlockSize():block.BlockSize()+len(ivSep)], ivSep) {
		return nil, terror.ErrCiphertextContextNotValid.Generate()
	}

	iv := ciphertext[:block.BlockSize()]
	ciphertext = ciphertext[block.BlockSize()+len(ivSep):]
	plaintext := make([]byte, len(ciphertext))

	stream := cipher.NewCFBDecrypter(block, iv)
	stream.XORKeyStream(plaintext, ciphertext)

	return plaintext, nil
}
