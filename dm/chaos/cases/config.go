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

package main

import (
	"flag"
	"time"

	config2 "github.com/pingcap/tiflow/dm/config"
)

// config is used to run chaos tests.
type config struct {
	*flag.FlagSet `toml:"-" yaml:"-" json:"-"`

	ConfigDir  string        `toml:"config-dir" yaml:"config-dir" json:"config-dir"`
	MasterAddr string        `toml:"master-addr" yaml:"master-addr" json:"master-addr"`
	Duration   time.Duration `toml:"duration" yaml:"duration" json:"duration"`

	MasterCount int `toml:"master-count" yaml:"master-count" json:"master-count"`
	WorkerCount int `toml:"worker-count" yaml:"worker-count" json:"worker-count"`

	Source1 config2.DBConfig `toml:"source-1" yaml:"source-1" json:"source-1"`
	Source2 config2.DBConfig `toml:"source-2" yaml:"source-2" json:"source-2"`
	Source3 config2.DBConfig `toml:"source-3" yaml:"source-3" json:"source-3"`
	Target  config2.DBConfig `toml:"target" yaml:"target" json:"target"`
}

// newConfig creates a config for this chaos testing suite.
func newConfig() *config {
	cfg := &config{}
	cfg.FlagSet = flag.NewFlagSet("chaos-case", flag.ContinueOnError)
	fs := cfg.FlagSet

	fs.StringVar(&cfg.ConfigDir, "config-dir", "/", "path of the source and task config files")
	fs.StringVar(&cfg.MasterAddr, "master-addr", "dm-master:8261", "address of dm-master")
	fs.DurationVar(&cfg.Duration, "duration", 20*time.Minute, "duration of cases running")

	fs.IntVar(&cfg.MasterCount, "master-count", 3, "expect count of dm-master")
	fs.IntVar(&cfg.WorkerCount, "worker-count", 3, "expect count of dm-worker")

	fs.StringVar(&cfg.Source1.Host, "source1.host", "mysql57-0.sources", "host of the first upstream source")
	fs.IntVar(&cfg.Source1.Port, "source1.port", 3306, "port of the first upstream source")
	fs.StringVar(&cfg.Source1.User, "source1.user", "root", "user of the first upstream source")
	fs.StringVar(&cfg.Source1.Password, "source1.password", "", "password of the first upstream source")

	fs.StringVar(&cfg.Source2.Host, "source2.host", "mysql8-0.sources", "host of the second upstream source")
	fs.IntVar(&cfg.Source2.Port, "source2.port", 3306, "port of the second upstream source")
	fs.StringVar(&cfg.Source2.User, "source2.user", "root", "user of the second upstream source")
	fs.StringVar(&cfg.Source2.Password, "source2.password", "", "password of the second upstream source")

	fs.StringVar(&cfg.Source3.Host, "source3.host", "mariadb-0.sources", "host of the third upstream source")
	fs.IntVar(&cfg.Source3.Port, "source3.port", 3306, "port of the third upstream source")
	fs.StringVar(&cfg.Source3.User, "source3.user", "root", "user of the third upstream source")
	fs.StringVar(&cfg.Source3.Password, "source3.password", "", "password of the third upstream source")

	fs.StringVar(&cfg.Target.Host, "target.host", "tidb-0.tidb", "host of the downstream target")
	fs.IntVar(&cfg.Target.Port, "target.port", 4000, "port of the downstream target")
	fs.StringVar(&cfg.Target.User, "target.user", "root", "user of the downstream target")
	fs.StringVar(&cfg.Target.Password, "target.password", "", "password of the downstream target")

	return cfg
}

// parse parses flag definitions from the argument list.
func (c *config) parse(args []string) error {
	if err := c.FlagSet.Parse(args); err != nil {
		return err
	}

	c.adjust()
	return nil
}

func (c *config) adjust() {
	// `go-sqlsmith` may generate ZERO time data, so we simply clear `sql_mode` now.
	// mysql:5.7 `explicit_defaults_for_timestamp`: OFF
	// tidb `explicit_defaults_for_timestamp`: ON
	// `ALTER TABLE .* ADD COLUMN (.* TIMESTAMP)` will have different default value
	c.Source1.Session = map[string]string{"sql_mode": "", "explicit_defaults_for_timestamp": "on"}
	c.Source2.Session = map[string]string{"sql_mode": "", "explicit_defaults_for_timestamp": "on"}
	c.Source3.Session = map[string]string{"sql_mode": ""} // explicit_defaults_for_timestamp enabled when deploy it

	c.Source1.Adjust()
	c.Source2.Adjust()
	c.Source3.Adjust()
	c.Target.Adjust()
}
