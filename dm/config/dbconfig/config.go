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

package dbconfig

import (
	"bytes"
	"encoding/json"
	"strings"

	"github.com/BurntSushi/toml"
	"github.com/pingcap/tiflow/dm/config/security"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/dm/pkg/terror"
	"github.com/pingcap/tiflow/dm/pkg/utils"
	"go.uber.org/zap"
)

// AdjustDBTimeZone force adjust session `time_zone`.
func AdjustDBTimeZone(config *DBConfig, timeZone string) {
	for k, v := range config.Session {
		if strings.ToLower(k) == "time_zone" {
			if v != timeZone {
				log.L().Warn("session variable 'time_zone' is overwritten by task config's timezone",
					zap.String("time_zone", config.Session[k]))
				config.Session[k] = timeZone
			}
			return
		}
	}
	if config.Session == nil {
		config.Session = make(map[string]string, 1)
	}
	config.Session["time_zone"] = timeZone
}

// RawDBConfig contains some low level database config.
type RawDBConfig struct {
	MaxIdleConns int
	ReadTimeout  string
	WriteTimeout string
}

// SetReadTimeout set readTimeout for raw database config.
func (c *RawDBConfig) SetReadTimeout(readTimeout string) *RawDBConfig {
	c.ReadTimeout = readTimeout
	return c
}

// SetWriteTimeout set writeTimeout for raw database config.
func (c *RawDBConfig) SetWriteTimeout(writeTimeout string) *RawDBConfig {
	c.WriteTimeout = writeTimeout
	return c
}

// SetMaxIdleConns set maxIdleConns for raw database config
// set value <= 0 then no idle connections are retained.
// set value > 0 then `value` idle connections are retained.
func (c *RawDBConfig) SetMaxIdleConns(value int) *RawDBConfig {
	c.MaxIdleConns = value
	return c
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

	RawDBCfg *RawDBConfig `toml:"-" json:"-" yaml:"-"`
	Net      string       `toml:"-" json:"-" yaml:"-"`
}

var defaultMaxIdleConns = 2

// DefaultRawDBConfig returns a default raw database config.
func DefaultRawDBConfig() *RawDBConfig {
	return &RawDBConfig{
		MaxIdleConns: defaultMaxIdleConns,
	}
}

func (db *DBConfig) String() string {
	cfg, err := json.Marshal(db)
	if err != nil {
		log.L().Error("fail to marshal config to json", log.ShortError(err))
	}
	return string(cfg)
}

// Toml returns TOML format representation of config.
func (db *DBConfig) Toml() (string, error) {
	var b bytes.Buffer
	enc := toml.NewEncoder(&b)
	if err := enc.Encode(db); err != nil {
		return "", terror.ErrConfigTomlTransform.Delegate(err, "encode db config to toml")
	}
	return b.String(), nil
}

// Decode loads config from file data.
func (db *DBConfig) Decode(data string) error {
	_, err := toml.Decode(data, db)
	return terror.ErrConfigTomlTransform.Delegate(err, "decode db config")
}

// Adjust adjusts the config.
func (db *DBConfig) Adjust() {
	if len(db.Password) > 0 {
		db.Password = utils.DecryptOrPlaintext(db.Password)
	}
}

func (db *DBConfig) AdjustWithTimeZone(timeZone string) {
	if timeZone != "" {
		AdjustDBTimeZone(db, timeZone)
	}
	db.Adjust()
}

// Clone returns a deep copy of DBConfig. This function only fixes data race when adjusting Session.
func (db *DBConfig) Clone() *DBConfig {
	if db == nil {
		return nil
	}

	clone := *db

	if db.MaxAllowedPacket != nil {
		packet := *(db.MaxAllowedPacket)
		clone.MaxAllowedPacket = &packet
	}

	if db.Session != nil {
		clone.Session = make(map[string]string, len(db.Session))
		for k, v := range db.Session {
			clone.Session[k] = v
		}
	}

	clone.Security = db.Security.Clone()

	if db.RawDBCfg != nil {
		dbCfg := *(db.RawDBCfg)
		clone.RawDBCfg = &dbCfg
	}

	return &clone
}
