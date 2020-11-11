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
	"net/http"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cdc/kv"
	"github.com/pingcap/ticdc/pkg/retry"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/pkg/logutil"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
)

var pd = flag.String("pd", "http://127.0.0.1:2379", "PD address and port")

func main() {
	flag.Parse()
	log.Info("table mover started")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	cluster, err := newCluster(ctx, *pd)
	if err != nil {
		log.Fatal("failed to create cluster info", zap.Error(err))
	}

	ticker := time.NewTicker(15 * time.Second)
	for {
		select {
		case <-ctx.Done():
			log.Info("Exiting", zap.Error(ctx.Err()))
			return
		case <-ticker.C:
			err := retry.Run(100*time.Millisecond, 20, func() error {
				return cluster.refreshInfo(ctx)
			})

			if err != nil {
				log.Warn("error refreshing cluster info", zap.Error(err))
			}

			if len(cluster.captures) <= 1 {
				log.Warn("no enough captures", zap.Reflect("captures", cluster.captures))
				continue
			}

			var (
				tableID       int64
				sourceCapture string
				targetCapture string
				changefeed    string
			)
			for capture, tables := range cluster.captures {
				if len(tables) == 0 {
					continue
				}

				tableID = tables[0].ID
				sourceCapture = capture
				changefeed = tables[0].changefeed
			}

			if tableID == 0 {
				log.Warn("no table", zap.Reflect("captures", cluster.captures))
				continue
			}

			for capture := range cluster.captures {
				if capture != sourceCapture {
					targetCapture = sourceCapture
				}
			}

			if targetCapture == "" {
				log.Fatal("no target, unexpected")
			}

			err = moveTable(ctx, cluster.ownerAddr, changefeed, targetCapture, tableID)
			if err != nil {
				log.Warn("failed to move table", zap.Error(err))
			}

			log.Info("moved table successful", zap.Int64("tableID", tableID))
		}
	}

}

type tableInfo struct {
	ID         int64
	changefeed string
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

	return ret, nil
}

func (c *cluster) refreshInfo(ctx context.Context) error {
	ownerID, err := c.cdcEtcdCli.GetOwnerID(ctx, kv.CaptureOwnerKey)
	if err != nil {
		return errors.Trace(err)
	}

	captureInfo, err := c.cdcEtcdCli.GetCaptureInfo(ctx, ownerID)
	if err != nil {
		return errors.Trace(err)
	}

	c.ownerAddr = captureInfo.AdvertiseAddr

	_, changefeeds, err := c.cdcEtcdCli.GetChangeFeeds(ctx)
	if err != nil {
		return errors.Trace(err)
	}
	if len(changefeeds) == 0 {
		return errors.New("No changefeed")
	}

	var changefeed string
	for k := range changefeeds {
		changefeed = k
		break
	}

	allTasks, err := c.cdcEtcdCli.GetAllTaskStatus(ctx, changefeed)
	if err != nil {
		return errors.Trace(err)
	}

	c.captures = make(map[string][]*tableInfo)
	for capture, taskInfo := range allTasks {
		c.captures[capture] = make([]*tableInfo, len(taskInfo.Tables))
		for tableID := range taskInfo.Tables {
			c.captures[capture] = append(c.captures[capture], &tableInfo{
				ID:         tableID,
				changefeed: changefeed,
			})
		}
	}

	return nil
}

func moveTable(ctx context.Context, ownerAddr string, changefeed string, target string, tableID int64) error {
	formStr := fmt.Sprintf("cf-id=%s&target-cp-id=%s&table-id=%d", changefeed, target, tableID)
	rd := bytes.NewReader([]byte(formStr))
	req, err := http.NewRequestWithContext(ctx, "POST", ownerAddr+"/capture/owner/move_table", rd)
	if err != nil {
		return errors.Trace(err)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return errors.Trace(err)
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return errors.New(resp.Status)
	}

	return nil
}
