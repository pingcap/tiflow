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

package util

import (
	"flag"
	"fmt"

	"github.com/BurntSushi/toml"
	"github.com/pingcap/errors"
)

// NewConfig creates a new config.
func NewConfig() *Config {
	cfg := &Config{}
	cfg.FlagSet = flag.NewFlagSet("ticdcTest", flag.ContinueOnError)
	fs := cfg.FlagSet

	fs.StringVar(&cfg.configFile, "config", "", "Config file")
	fs.IntVar(&cfg.WorkerCount, "c", 1, "parallel worker count")
	fs.IntVar(&cfg.JobCount, "n", 1, "total job count")
	fs.IntVar(&cfg.Batch, "b", 1, "insert batch commit count")
	fs.StringVar(&cfg.LogLevel, "L", "info", "log level: debug, info, warn, error, fatal")

	return cfg
}

// Config is the configuration.
type Config struct {
	*flag.FlagSet `json:"-"`

	LogLevel string `toml:"log-level" json:"log-level"`

	WorkerCount int `toml:"worker-count" json:"worker-count"`

	JobCount int `toml:"job-count" json:"job-count"`

	Batch int `toml:"batch" json:"batch"`

	PDAddr string `toml:"pd" json:"pd"`

	SourceDBCfg []DBConfig `toml:"source-db" json:"source-db"`

	TargetDBCfg DBConfig `toml:"target-db" json:"target-db"`

	configFile string
}

// Parse parses flag definitions from the argument list.
func (c *Config) Parse(arguments []string) error {
	// Parse first to get config file.
	err := c.FlagSet.Parse(arguments)
	if err != nil {
		return errors.Trace(err)
	}

	// Load config file if specified.
	if c.configFile != "" {
		err = c.configFromFile(c.configFile)
		if err != nil {
			return errors.Trace(err)
		}
	}

	// Parse again to replace with command line options.
	err = c.FlagSet.Parse(arguments)
	if err != nil {
		return errors.Trace(err)
	}

	if len(c.FlagSet.Args()) != 0 {
		return errors.Errorf("'%s' is an invalid flag", c.FlagSet.Arg(0))
	}

	return nil
}

func (c *Config) String() string {
	if c == nil {
		return "<nil>"
	}
	return fmt.Sprintf("Config(%+v)", *c)
}

// configFromFile loads config from file.
func (c *Config) configFromFile(path string) error {
	_, err := toml.DecodeFile(path, c)
	return errors.Trace(err)
}
