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

// This is a program that drives the CDC cluster to move a table
package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/kv"
	cerrors "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/retry"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/pkg/logutil"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
)

var (
	pd       = flag.String("pd", "http://127.0.0.1:2379", "PD address and port")
	logLevel = flag.String("log-level", "debug", "Set log level of the logger")
)

func main() {
	flag.Parse()
	if strings.ToLower(*logLevel) == "debug" {
		log.SetLevel(zapcore.DebugLevel)
	}

	log.Info("table mover started")
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	defer cancel()

	cluster, err := newCluster(ctx, *pd)
	if err != nil {
		log.Fatal("failed to create cluster info", zap.Error(err))
	}
	err = retry.Do(ctx, func() error {
		err := cluster.refreshInfo(ctx)
		if err != nil {
			log.Warn("error refreshing cluster info", zap.Error(err))
		}

		log.Info("task status", zap.Reflect("status", cluster.captures))

		if len(cluster.captures) <= 1 {
			return errors.New("too few captures")
		}
		return nil
	}, retry.WithBackoffBaseDelay(100), retry.WithMaxTries(20), retry.WithIsRetryableErr(cerrors.IsRetryableError))

	if err != nil {
		log.Fatal("Fail to get captures", zap.Error(err))
	}

	var sourceCapture string

	for capture, tables := range cluster.captures {
		if len(tables) == 0 {
			continue
		}
		sourceCapture = capture
		break
	}

	var targetCapture string

	for candidateCapture := range cluster.captures {
		if candidateCapture != sourceCapture {
			targetCapture = candidateCapture
		}
	}

	if targetCapture == "" {
		log.Fatal("no target, unexpected")
	}

	// move all tables to another capture
	for _, table := range cluster.captures[sourceCapture] {
		err = moveTable(ctx, cluster.ownerAddr, table.Changefeed, targetCapture, table.ID)
		if err != nil {
			log.Warn("failed to move table", zap.Error(err))
			continue
		}

		log.Info("moved table successful", zap.Int64("tableID", table.ID))
	}

	log.Info("all tables are moved", zap.String("sourceCapture", sourceCapture), zap.String("targetCapture", targetCapture))

	for counter := 0; counter < 30; counter++ {
		err := retry.Do(ctx, func() error {
			return cluster.refreshInfo(ctx)
		}, retry.WithBackoffBaseDelay(100), retry.WithMaxTries(5+1), retry.WithIsRetryableErr(cerrors.IsRetryableError))
		if err != nil {
			log.Warn("error refreshing cluster info", zap.Error(err))
		}

		tables, ok := cluster.captures[sourceCapture]
		if !ok {
			log.Warn("source capture is gone", zap.String("sourceCapture", sourceCapture))
			break
		}

		if len(tables) == 0 {
			log.Info("source capture is now empty", zap.String("sourceCapture", sourceCapture))
			break
		}

		if counter != 30 {
			log.Debug("source capture is not empty, will try again", zap.String("sourceCapture", sourceCapture))
			time.Sleep(time.Second * 10)
		}
	}
}

type tableInfo struct {
	ID         int64
	Changefeed string
}

type cluster struct {
	ownerAddr  string
	captures   map[string][]*tableInfo
	cdcEtcdCli kv.CDCEtcdClient
}

func newCluster(ctx context.Context, pd string) (*cluster, error) {
	logConfig := logutil.DefaultZapLoggerConfig
	logConfig.Level = zap.NewAtomicLevelAt(zapcore.ErrorLevel)

	etcdCli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{pd},
		TLS:         nil,
		Context:     ctx,
		LogConfig:   &logConfig,
		DialTimeout: 5 * time.Second,
		DialOptions: []grpc.DialOption{
			grpc.WithInsecure(),
			grpc.WithBlock(),
			grpc.WithConnectParams(grpc.ConnectParams{
				Backoff: backoff.Config{
					BaseDelay:  time.Second,
					Multiplier: 1.1,
					Jitter:     0.1,
					MaxDelay:   3 * time.Second,
				},
				MinConnectTimeout: 3 * time.Second,
			}),
		},
	})
	if err != nil {
		return nil, errors.Trace(err)
	}

	ret := &cluster{
		ownerAddr:  "",
		captures:   nil,
		cdcEtcdCli: kv.NewCDCEtcdClient(ctx, etcdCli),
	}

	log.Info("new cluster initialized")

	return ret, nil
}

func (c *cluster) refreshInfo(ctx context.Context) error {
	ownerID, err := c.cdcEtcdCli.GetOwnerID(ctx, kv.CaptureOwnerKey)
	if err != nil {
		return errors.Trace(err)
	}

	log.Debug("retrieved owner ID", zap.String("ownerID", ownerID))

	captureInfo, err := c.cdcEtcdCli.GetCaptureInfo(ctx, ownerID)
	if err != nil {
		return errors.Trace(err)
	}

	log.Debug("retrieved owner addr", zap.String("ownerAddr", captureInfo.AdvertiseAddr))
	c.ownerAddr = captureInfo.AdvertiseAddr

	_, changefeeds, err := c.cdcEtcdCli.GetChangeFeeds(ctx)
	if err != nil {
		return errors.Trace(err)
	}
	if len(changefeeds) == 0 {
		return errors.New("No changefeed")
	}

	log.Debug("retrieved changefeeds", zap.Reflect("changefeeds", changefeeds))

	var changefeed string
	for k := range changefeeds {
		changefeed = k
		break
	}

	c.captures = make(map[string][]*tableInfo)
	_, captures, err := c.cdcEtcdCli.GetCaptures(ctx)
	if err != nil {
		return errors.Trace(err)
	}
	for _, capture := range captures {
		c.captures[capture.ID] = make([]*tableInfo, 0)
	}

	allTasks, err := c.cdcEtcdCli.GetAllTaskStatus(ctx, changefeed)
	if err != nil {
		return errors.Trace(err)
	}

	log.Debug("retrieved all tasks", zap.Reflect("tasks", allTasks))

	for capture, taskInfo := range allTasks {
		if _, ok := c.captures[capture]; !ok {
			c.captures[capture] = make([]*tableInfo, 0, len(taskInfo.Tables))
		}

		for tableID := range taskInfo.Tables {
			c.captures[capture] = append(c.captures[capture], &tableInfo{
				ID:         tableID,
				Changefeed: changefeed,
			})
		}
	}

	return nil
}

func moveTable(ctx context.Context, ownerAddr string, changefeed string, target string, tableID int64) error {
	formStr := fmt.Sprintf("cf-id=%s&target-cp-id=%s&table-id=%d", changefeed, target, tableID)
	log.Debug("preparing HTTP API call to owner", zap.String("formStr", formStr))
	rd := bytes.NewReader([]byte(formStr))
	req, err := http.NewRequestWithContext(ctx, "POST", "http://"+ownerAddr+"/capture/owner/move_table", rd)
	if err != nil {
		return errors.Trace(err)
	}

	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return errors.Trace(err)
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return errors.Trace(err)
		}
		log.Warn("http error", zap.ByteString("body", body))
		return errors.New(resp.Status)
	}

	return nil
}
