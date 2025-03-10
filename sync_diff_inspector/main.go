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

package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tiflow/sync_diff_inspector/config"
	"github.com/pingcap/tiflow/sync_diff_inspector/diff"
	flag "github.com/spf13/pflag"
	"go.uber.org/zap"
)

func init() {
	c := &charset.Charset{
		Name:             "gbk",
		DefaultCollation: "gbk_chinese_ci",
		Collations:       map[string]*charset.Collation{},
		Maxlen:           2,
	}
	charset.AddCharset(c)
	for _, coll := range charset.GetSupportedCollations() {
		if strings.EqualFold(coll.CharsetName, c.Name) {
			charset.AddCollation(coll)
		}
	}
}

func main() {
	cfg := config.NewConfig()
	err := cfg.Parse(os.Args[1:])
	switch errors.Cause(err) {
	case nil:
	case flag.ErrHelp:
		os.Exit(0)
	default:
		fmt.Printf("Error: %s\n", err.Error())
		cfg.FlagSet.PrintDefaults()
		os.Exit(2)
	}

	if cfg.PrintVersion {
		fmt.Print(util.GetRawInfo("sync_diff_inspector"))
		return
	}

	if cfg.Template != "" {
		if err := config.ExportTemplateConfig(cfg.Template); err != nil {
			fmt.Printf("%s\n", err.Error())
			os.Exit(2)
		}
		return
	}

	conf := new(log.Config)
	conf.Level = cfg.LogLevel

	conf.File.Filename = filepath.Join(cfg.Task.OutputDir, config.LogFileName)
	lg, p, e := log.InitLogger(conf)
	if e != nil {
		log.Error("Log init failed!", zap.String("error", e.Error()))
		os.Exit(2)
	}
	log.ReplaceGlobals(lg, p)

	util.PrintInfo("sync_diff_inspector")

	// Initial config
	err = cfg.Init()
	if err != nil {
		fmt.Printf("Fail to initialize config.\n%s\n", err.Error())
		os.Exit(2)
	}

	ok := cfg.CheckConfig()
	if !ok {
		fmt.Printf("There is something wrong with your config, please check log info in %s\n", conf.File.Filename)
		os.Exit(2)
	}

	log.Info("", zap.Stringer("config", cfg))

	ctx := context.Background()
	if !checkSyncState(ctx, cfg) {
		log.Warn("check failed!!!")
		os.Exit(1)
	}
	log.Info("check pass!!!")
}

func checkSyncState(ctx context.Context, cfg *config.Config) bool {
	beginTime := time.Now()
	defer func() {
		log.Info("check data finished", zap.Duration("cost", time.Since(beginTime)))
	}()

	d, err := diff.NewDiff(ctx, cfg)
	if err != nil {
		fmt.Printf("An error occurred while initializing diff: %s, please check log info in %s for full details\n",
			err, filepath.Join(cfg.Task.OutputDir, config.LogFileName))
		log.Fatal("failed to initialize diff process", zap.Error(err))
		return false
	}
	defer d.Close()

	if !cfg.CheckDataOnly {
		err = d.StructEqual(ctx)
		if err != nil {
			fmt.Printf("An error occurred while comparing table structure: %s, please check log info in %s for full details\n",
				err, filepath.Join(cfg.Task.OutputDir, config.LogFileName))
			log.Fatal("failed to check structure difference", zap.Error(err))
			return false
		}
	} else {
		log.Info("Check table data only, skip struct check")
	}
	if !cfg.CheckStructOnly {
		err = d.Equal(ctx)
		if err != nil {
			fmt.Printf("An error occurred while comparing table data: %s, please check log info in %s for full details\n",
				err, filepath.Join(cfg.Task.OutputDir, config.LogFileName))
			log.Fatal("failed to check data difference", zap.Error(err))
			return false
		}
	} else {
		log.Info("Check table struct only, skip data check")
	}
	return d.PrintSummary(ctx)
}
