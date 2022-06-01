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

package main

import (
	"flag"
	"time"
)

// config is used to run chaos tests.
type config struct {
	*flag.FlagSet `toml:"-" yaml:"-" json:"-"`

	MasterAddr string `toml:"master-addr" yaml:"master-addr" json:"master-addr"`
	// reuse user metastore currently
	EtcdAddr string        `toml:"etcd-addr" yaml:"etcd-addr" json:"etcd-addr"`
	Duration time.Duration `toml:"duration" yaml:"duration" json:"duration"`

	MasterCount int `toml:"master-count" yaml:"master-count" json:"master-count"`
	WorkerCount int `toml:"worker-count" yaml:"worker-count" json:"worker-count"`
}

// newConfig creates a config for this chaos testing suite.
func newConfig() *config {
	cfg := &config{}
	cfg.FlagSet = flag.NewFlagSet("chaos-case", flag.ContinueOnError)
	fs := cfg.FlagSet

	fs.StringVar(&cfg.MasterAddr, "master-addr", "server-master:10240", "address of server-master")
	fs.StringVar(&cfg.EtcdAddr, "etcd-addr", "metastore:12479", "address of etcd server(used by fake job)")
	fs.DurationVar(&cfg.Duration, "duration", 20*time.Minute, "duration of cases running")

	fs.IntVar(&cfg.MasterCount, "master-count", 3, "expect count of server-master")
	fs.IntVar(&cfg.WorkerCount, "worker-count", 4, "expect count of executor")

	return cfg
}

// parse parses flag definitions from the argument list.
func (c *config) parse(args []string) error {
	return c.FlagSet.Parse(args)
}
