// Copyright 2022 PingCAP, Inc.
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

package servermaster

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/BurntSushi/toml"
	validation "github.com/go-ozzo/ozzo-validation/v4"
	"github.com/pingcap/log"
	resModel "github.com/pingcap/tiflow/engine/pkg/externalresource/model"
	metaModel "github.com/pingcap/tiflow/engine/pkg/meta/model"
	"github.com/pingcap/tiflow/engine/servermaster/jobop"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/logutil"
	"github.com/pingcap/tiflow/pkg/security"
	"go.uber.org/zap"
)

const (
	defaultKeepAliveTTL      = "20s"
	defaultKeepAliveInterval = "500ms"
	defaultMetricInterval    = 15 * time.Second
	defaultMasterAddr        = "127.0.0.1:10240"

	// DefaultBusinessMetaID is the ID for default business metastore
	DefaultBusinessMetaID        = "_default"
	defaultBusinessMetaEndpoints = "127.0.0.1:3336"
	defaultBusinessMetaUser      = "root"
	defaultBusinessMetaPassword  = ""
	defaultBusinessMetaSchema    = "test_business"

	// FrameMetaID is the ID for frame metastore
	FrameMetaID               = "_root"
	defaultFrameMetaEndpoints = "127.0.0.1:3336"
	defaultFrameMetaUser      = "root"
	defaultFrameMetaPassword  = ""
	defaultFrameMetaSchema    = "test_framework"

	defaultFrameworkStoreType = metaModel.StoreTypeMySQL
	defaultBusinessStoreType  = metaModel.StoreTypeMySQL
)

// Config is the configuration for server-master.
type Config struct {
	LogConf logutil.Config `toml:"log" json:"log"`

	Name          string `toml:"name" json:"name"`
	Addr          string `toml:"addr" json:"addr"`
	AdvertiseAddr string `toml:"advertise-addr" json:"advertise-addr"`

	FrameworkMeta *metaModel.StoreConfig `toml:"framework-meta" json:"framework-meta"`
	BusinessMeta  *metaModel.StoreConfig `toml:"business-meta" json:"business-meta"`

	KeepAliveTTLStr string `toml:"keepalive-ttl" json:"keepalive-ttl"`
	// time interval string to check executor aliveness
	KeepAliveIntervalStr string `toml:"keepalive-interval" json:"keepalive-interval"`

	KeepAliveTTL      time.Duration `toml:"-" json:"-"`
	KeepAliveInterval time.Duration `toml:"-" json:"-"`

	Storage resModel.Config `toml:"storage" json:"storage"`

	Security *security.Credential `toml:"security" json:"security"`

	JobBackoff *jobop.BackoffConfig `toml:"job-backoff" json:"job-backoff"`
}

func (c *Config) String() string {
	cfg, err := json.Marshal(c)
	if err != nil {
		log.Error("marshal to json", zap.Reflect("master config", c), logutil.ShortError(err))
	}
	return string(cfg)
}

// Toml returns TOML format representation of config.
func (c *Config) Toml() (string, error) {
	var b bytes.Buffer

	err := toml.NewEncoder(&b).Encode(c)
	if err != nil {
		log.Error("fail to marshal config to toml", logutil.ShortError(err))
	}

	return b.String(), nil
}

// AdjustAndValidate validates and adjusts the master configuration
func (c *Config) AdjustAndValidate() (err error) {
	// adjust the metastore type
	c.FrameworkMeta.StoreType = strings.ToLower(strings.TrimSpace(c.FrameworkMeta.StoreType))
	c.BusinessMeta.StoreType = strings.ToLower(strings.TrimSpace(c.BusinessMeta.StoreType))

	if c.FrameworkMeta.Schema == defaultFrameMetaSchema {
		log.Warn("use default schema for framework metastore, "+
			"better to use predefined schema in production environment",
			zap.String("schema", defaultFrameMetaSchema))
	}
	if c.BusinessMeta.Schema == defaultBusinessMetaSchema {
		log.Warn("use default schema for business metastore, "+
			"better to use predefined schema in production environment",
			zap.String("schema", defaultBusinessMetaSchema))
	}

	if c.AdvertiseAddr == "" {
		c.AdvertiseAddr = c.Addr
	}

	if c.Name == "" {
		c.Name = fmt.Sprintf("master-%s", c.AdvertiseAddr)
	}

	c.KeepAliveInterval, err = time.ParseDuration(c.KeepAliveIntervalStr)
	if err != nil {
		return err
	}

	c.KeepAliveTTL, err = time.ParseDuration(c.KeepAliveTTLStr)
	if err != nil {
		return err
	}

	if err := validation.ValidateStruct(c, validation.Field(&c.Storage)); err != nil {
		return err
	}

	return validation.ValidateStruct(c,
		validation.Field(&c.FrameworkMeta),
		validation.Field(&c.BusinessMeta),
	)
}

func (c *Config) configFromString(data string) error {
	metaData, err := toml.Decode(data, c)
	if err != nil {
		return errors.WrapError(errors.ErrMasterDecodeConfigFile, err)
	}
	return checkUndecodedItems(metaData)
}

// GetDefaultMasterConfig returns a default master config
func GetDefaultMasterConfig() *Config {
	return &Config{
		LogConf: logutil.Config{
			Level: "info",
			File:  "",
		},
		Name:                 "",
		Addr:                 defaultMasterAddr,
		AdvertiseAddr:        "",
		FrameworkMeta:        newFrameMetaConfig(),
		BusinessMeta:         NewDefaultBusinessMetaConfig(),
		KeepAliveTTLStr:      defaultKeepAliveTTL,
		KeepAliveIntervalStr: defaultKeepAliveInterval,
		JobBackoff:           jobop.NewDefaultBackoffConfig(),
		Storage:              resModel.DefaultConfig,
	}
}

func checkUndecodedItems(metaData toml.MetaData) error {
	undecoded := metaData.Undecoded()
	if len(undecoded) > 0 {
		var undecodedItems []string
		for _, item := range undecoded {
			undecodedItems = append(undecodedItems, item.String())
		}
		return errors.ErrMasterConfigUnknownItem.GenWithStackByArgs(strings.Join(undecodedItems, ","))
	}
	return nil
}

// newFrameMetaConfig return the default framework metastore config
func newFrameMetaConfig() *metaModel.StoreConfig {
	conf := metaModel.DefaultStoreConfig()
	conf.Schema = defaultFrameMetaSchema
	conf.StoreID = FrameMetaID
	conf.StoreType = defaultFrameworkStoreType
	conf.Endpoints = append(conf.Endpoints, defaultFrameMetaEndpoints)
	conf.User = defaultFrameMetaUser
	conf.Password = defaultFrameMetaPassword

	return conf
}

// NewDefaultBusinessMetaConfig return the default business metastore config
func NewDefaultBusinessMetaConfig() *metaModel.StoreConfig {
	conf := metaModel.DefaultStoreConfig()
	conf.Schema = defaultBusinessMetaSchema
	conf.StoreID = DefaultBusinessMetaID
	conf.StoreType = defaultBusinessStoreType
	conf.Endpoints = append(conf.Endpoints, defaultBusinessMetaEndpoints)
	conf.User = defaultBusinessMetaUser
	conf.Password = defaultBusinessMetaPassword

	return conf
}
