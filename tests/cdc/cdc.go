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
	"os"

	_ "github.com/go-sql-driver/mysql"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb-cdc/tests/util"
)

func main() {
	cfg := util.NewConfig()
	err := cfg.Parse(os.Args[1:])
	switch errors.Cause(err) {
	case nil:
	case flag.ErrHelp:
		os.Exit(0)
	default:
		log.S().Errorf("parse cmd flags err %s\n", err)
		os.Exit(2)
	}

	sourceDB, err := util.CreateDB(cfg.SourceDBCfg)
	if err != nil {
		log.S().Fatal(err)
	}
	defer func() {
		if err := util.CloseDB(sourceDB); err != nil {
			log.S().Errorf("Failed to close source database: %s\n", err)
		}
	}()

	targetDB, err := util.CreateDB(cfg.TargetDBCfg)
	if err != nil {
		log.S().Fatal(err)
	}
	defer func() {
		if err := util.CloseDB(targetDB); err != nil {
			log.S().Errorf("Failed to close target database: %s\n", err)
		}
	}()

	sourceDBs, err := util.CreateSourceDBs()
	if err != nil {
		log.S().Fatal(err)
	}
	defer func() {
		if err := util.CloseDBs(sourceDBs); err != nil {
			log.S().Errorf("Failed to close source databases: %s\n", err)
		}
	}()

	//dailytest.RunMultiSource(sourceDBs, targetDB, cfg.SourceDBCfg.Name)
	//dailytest.Run(sourceDB, targetDB, cfg.SourceDBCfg.Name, cfg.WorkerCount, cfg.JobCount, cfg.Batch)
}
