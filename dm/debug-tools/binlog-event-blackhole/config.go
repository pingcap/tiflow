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

package main

import (
	"flag"

	"github.com/pingcap/errors"
)

// config is the configuration used by this binlog-event-blackhole.
type config struct {
	*flag.FlagSet

	logLevel  string
	logFile   string
	logFormat string

	mode int

	addr       string
	username   string
	password   string
	serverID   int
	binlogName string
	binlogPos  int
}

// newConfig creates a new config instance.
func newConfig() *config {
	cfg := &config{
		FlagSet: flag.NewFlagSet("binlog-event-blackhole", flag.ContinueOnError),
	}
	fs := cfg.FlagSet

	fs.StringVar(&cfg.logLevel, "L", "info", "log level: debug, info, warn, error, fatal")
	fs.StringVar(&cfg.logFile, "log-file", "", "log file path")
	fs.StringVar(&cfg.logFormat, "log-format", "text", `the format of the log, "text" or "json"`)

	fs.IntVar(&cfg.mode, "mode", 0, "event read mode.\n1: read packet with go-mysql;\n2: read packet without go-mysql;\n3: read binary data but do nothing")

	fs.StringVar(&cfg.addr, "addr", "", "master's address")
	fs.StringVar(&cfg.username, "u", "", "master's username")
	fs.StringVar(&cfg.password, "p", "", "password for `username`")
	fs.IntVar(&cfg.serverID, "server-id", 0, "slave's server-id")
	fs.StringVar(&cfg.binlogName, "binlog-name", "", "startup binlog filename")
	fs.IntVar(&cfg.binlogPos, "binlog-pos", 4, "startup binlog position")

	return cfg
}

// parse parses flag definitions from the argument list.
func (c *config) parse(args []string) error {
	err := c.FlagSet.Parse(args)
	return errors.Trace(err)
}
