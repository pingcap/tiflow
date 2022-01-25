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

package common

import (
	"encoding/json"
	"fmt"
	"net"
	"os"
	"strings"
	"time"

	"github.com/google/shlex"
	"go.uber.org/zap"

	"github.com/pingcap/tiflow/dm/dm/config"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/dm/pkg/utils"

	"github.com/BurntSushi/toml"
	"github.com/pingcap/errors"
	"github.com/spf13/pflag"
)

const (
	defaultRPCTimeout = "10m"

	// EncryptCmdName is special command.
	EncryptCmdName = "encrypt"
	// DecryptCmdName is special command.
	DecryptCmdName = "decrypt"

	// Master specifies member master type.
	Master = "master"
	// Worker specifies member worker type.
	Worker = "worker"

	dialTimeout      = 3 * time.Second
	keepaliveTimeout = 3 * time.Second
	keepaliveTime    = 3 * time.Second

	// DefaultErrorCnt represents default count of errors to display for check-task.
	DefaultErrorCnt = 10
	// DefaultWarnCnt represents count of warns to display for check-task.
	DefaultWarnCnt = 10
)

var argsNeedAdjust = [...]string{"-version", "-config", "-master-addr", "-rpc-timeout", "-ssl-ca", "-ssl-cert", "-ssl-key", "-" + EncryptCmdName, "-" + DecryptCmdName}

// NewConfig creates a new base config for dmctl.
func NewConfig(fs *pflag.FlagSet) *Config {
	cfg := &Config{}
	cfg.FlagSet = fs
	return cfg
}

// DefineConfigFlagSet defines flag definitions for configs.
func DefineConfigFlagSet(fs *pflag.FlagSet) {
	fs.BoolP("version", "V", false, "Prints version and exit.")
	fs.String("config", "", "Path to config file.")
	fs.String("master-addr", "", "Master API server address, this parameter is required when interacting with the dm-master")
	fs.String("rpc-timeout", defaultRPCTimeout, fmt.Sprintf("RPC timeout, default is %s.", defaultRPCTimeout))
	fs.String("ssl-ca", "", "Path of file that contains list of trusted SSL CAs for connection.")
	fs.String("ssl-cert", "", "Path of file that contains X509 certificate in PEM format for connection.")
	fs.String("ssl-key", "", "Path of file that contains X509 key in PEM format for connection.")
	fs.String(EncryptCmdName, "", "Encrypts plaintext to ciphertext.")
	fs.String(DecryptCmdName, "", "Decrypts ciphertext to plaintext.")
	_ = fs.MarkHidden(EncryptCmdName)
	_ = fs.MarkHidden(DecryptCmdName)
}

// AdjustArgumentsForPflags adjust flag format args to pflags format.
func AdjustArgumentsForPflags(args []string) []string {
	for i, arg := range args {
		arg = strings.TrimSpace(arg)
		for _, adjustArg := range argsNeedAdjust {
			// -master-addr 127.0.0.1:8261 and -master-addr=127.0.0.1:8261
			if arg == adjustArg || strings.HasPrefix(arg, adjustArg+"=") {
				args[i] = "-" + arg
			}
		}
	}
	return args
}

func (c *Config) getConfigFromFlagSet() error {
	var err error
	fs := c.FlagSet
	c.ConfigFile, err = fs.GetString("config")
	if err != nil {
		return err
	}
	c.MasterAddr, err = fs.GetString("master-addr")
	if err != nil {
		return err
	}
	c.RPCTimeoutStr, err = fs.GetString("rpc-timeout")
	if err != nil {
		return err
	}
	c.SSLCA, err = fs.GetString("ssl-ca")
	if err != nil {
		return err
	}
	c.SSLCert, err = fs.GetString("ssl-cert")
	if err != nil {
		return err
	}
	c.SSLKey, err = fs.GetString("ssl-key")
	return err
}

// Config is the configuration.
type Config struct {
	*pflag.FlagSet `json:"-"`

	MasterAddr string `toml:"master-addr" json:"master-addr"`

	RPCTimeoutStr string        `toml:"rpc-timeout" json:"rpc-timeout"`
	RPCTimeout    time.Duration `json:"-"`

	ConfigFile string `json:"config-file"`

	config.Security
}

func (c *Config) String() string {
	//nolint:staticcheck
	cfg, err := json.Marshal(c)
	if err != nil {
		fmt.Printf("marshal config to json error %v", err)
	}
	return string(cfg)
}

// Adjust parses flag definitions from the argument list.
func (c *Config) Adjust() error {
	err := c.getConfigFromFlagSet()
	if err != nil {
		return errors.Trace(err)
	}

	// Load config file if specified.
	if c.ConfigFile != "" {
		err = c.configFromFile(c.ConfigFile)
		if err != nil {
			return errors.Annotatef(err, "the current command parameter: --config is invalid or used incorrectly")
		}
	}

	// Parse again to replace with command line options.
	err = c.getConfigFromFlagSet()
	if err != nil {
		return errors.Trace(err)
	}

	// try get master Addr from env "DM_MASTER_ADDR" if this flag is empty.
	if c.MasterAddr == "" {
		c.MasterAddr = os.Getenv("DM_MASTER_ADDR")
	}
	if c.MasterAddr == "" {
		return errors.Errorf("--master-addr not provided, this parameter is required when interacting with the dm-master, you can also use environment variable 'DM_MASTER_ADDR' to specify the value. Use `dmctl --help` to see more help messages")
	}

	return errors.Trace(c.adjust())
}

// Validate check config is ready to execute command.
func (c *Config) Validate() error {
	if c.MasterAddr == "" {
		return errors.New("--master-addr not provided")
	}
	if err := validateAddr(c.MasterAddr); err != nil {
		return errors.Annotatef(err, "specify master addr %s", c.MasterAddr)
	}
	return nil
}

// configFromFile loads config from file.
func (c *Config) configFromFile(path string) error {
	_, err := toml.DecodeFile(path, c)
	return errors.Trace(err)
}

// adjust adjusts configs.
func (c *Config) adjust() error {
	if c.RPCTimeoutStr == "" {
		c.RPCTimeoutStr = defaultRPCTimeout
	}
	timeout, err := time.ParseDuration(c.RPCTimeoutStr)
	if err != nil {
		return errors.Trace(err)
	}
	if timeout <= time.Duration(0) {
		return errors.Errorf("invalid time duration: %s", c.RPCTimeoutStr)
	}
	c.RPCTimeout = timeout
	return nil
}

// validate host:port format address.
func validateAddr(addr string) error {
	endpoints := strings.Split(addr, ",")
	for _, endpoint := range endpoints {
		if _, _, err := net.SplitHostPort(utils.UnwrapScheme(endpoint)); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

// SplitArgsRespectQuote splits args by space, but won't split space inside single or double quotes.
func SplitArgsRespectQuote(line string) []string {
	ret, err := shlex.Split(line)
	if err != nil {
		log.L().Error("split args error", zap.Error(err))
		return []string{line}
	}
	return ret
}
