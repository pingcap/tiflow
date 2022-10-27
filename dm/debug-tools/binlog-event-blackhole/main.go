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
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"go.uber.org/zap"
)

func main() {
	cfg := newConfig()
	err := cfg.parse(os.Args[1:])
	switch errors.Cause(err) {
	case nil:
	case flag.ErrHelp:
		os.Exit(0)
	default:
		fmt.Printf("parse cmd flags err %s \n", err)
		os.Exit(2)
	}

	err = log.InitLogger(&log.Config{
		File:   cfg.logFile,
		Level:  strings.ToLower(cfg.logLevel),
		Format: cfg.logFormat,
	})
	if err != nil {
		fmt.Printf("init logger error %v", errors.ErrorStack(err))
		os.Exit(2)
	}

	conn, err := registerSlave(cfg.addr, cfg.username, cfg.password, uint32(cfg.serverID))
	if err != nil {
		log.L().Error("register slave", zap.Error(err))
		os.Exit(2)
	}
	log.L().Info("registered slave", zap.Uint32("connection ID", conn.GetConnectionID()))

	ctx, cancel := context.WithCancel(context.Background())

	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)
	go func() {
		sig := <-sc
		cancel()
		log.L().Info("got signal to exit", zap.Stringer("signal", sig))
		err2 := closeConn(conn)
		if err2 != nil {
			log.L().Error("close connection", zap.Error(err2))
		}
	}()

	err = startSync(conn, uint32(cfg.serverID), cfg.binlogName, uint32(cfg.binlogPos))
	if err != nil {
		log.L().Error("start sync", zap.Error(err))
		os.Exit(2)
	}
	log.L().Info("start sync",
		zap.Int("server-id", cfg.serverID), zap.String("binlog-name", cfg.binlogName),
		zap.Int("binlog-pos", cfg.binlogPos))

	var (
		eventCount uint64
		byteCount  uint64
		duration   time.Duration
	)
	switch cfg.mode {
	case 1:
		eventCount, byteCount, duration, err = readEventsWithGoMySQL(ctx, conn)
	case 2:
		eventCount, byteCount, duration, err = readEventsWithoutGoMySQL(ctx, conn)
	case 3:
		byteCount, duration, err = readDataOnly(ctx, conn)
	default:
		log.L().Error("invalid mode specified`", zap.Int("mode", cfg.mode))
	}
	if err != nil {
		log.L().Error("read events", zap.Error(err))
	}

	tps := float64(eventCount) / duration.Seconds()
	speed := float64(byteCount) / duration.Seconds()
	log.L().Info("binlog-event-blackhole exit",
		zap.Uint64("event-count", eventCount), zap.Uint64("byte-count", byteCount),
		zap.Duration("duration", duration), zap.Float64("tps", tps), zap.Float64("throughput (byte/s)", speed))
}
