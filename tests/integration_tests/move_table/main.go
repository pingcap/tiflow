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
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	cerrors "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/etcd"
	"github.com/pingcap/tiflow/pkg/httputil"
	"github.com/pingcap/tiflow/pkg/retry"
	"github.com/pingcap/tiflow/pkg/security"
	"go.etcd.io/etcd/client/pkg/v3/logutil"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
)

var (
	pd       = flag.String("pd", "http://127.0.0.1:2379", "PD address and port")
	logLevel = flag.String("log-level", "debug", "Set log level of the logger")
)

const (
	maxCheckSourceEmptyRetries = 30
)

// This program moves all tables replicated by a certain capture to other captures,
// and makes sure that the original capture becomes empty.
func main() {
	flag.Parse()
	if strings.ToLower(*logLevel) == "debug" {
		log.SetLevel(zapcore.DebugLevel)
	}

	log.Info("table mover started")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
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

	err = cluster.moveAllTables(ctx, sourceCapture, targetCapture)
	if err != nil {
		log.Fatal("failed to move tables", zap.Error(err))
	}

	log.Info("all tables are moved", zap.String("sourceCapture", sourceCapture), zap.String("targetCapture", targetCapture))
}

type tableInfo struct {
	ID         int64
	Changefeed string
}

type cluster struct {
	ownerAddr  string
	captures   map[string][]*tableInfo
	cdcEtcdCli etcd.CDCEtcdClient
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
		cdcEtcdCli: etcd.NewCDCEtcdClient(ctx, etcdCli),
	}

	log.Info("new cluster initialized")

	return ret, nil
}

func (c *cluster) moveAllTables(ctx context.Context, sourceCapture, targetCapture string) error {
	// move all tables to another capture
	for _, table := range c.captures[sourceCapture] {
		err := moveTable(ctx, c.ownerAddr, table.Changefeed, targetCapture, table.ID)
		if err != nil {
			log.Warn("failed to move table", zap.Error(err))
			continue
		}

		log.Info("moved table successful", zap.Int64("tableID", table.ID))
	}

	for counter := 0; counter < maxCheckSourceEmptyRetries; counter++ {
		err := retry.Do(ctx, func() error {
			return c.refreshInfo(ctx)
		}, retry.WithBackoffBaseDelay(100), retry.WithMaxTries(5+1), retry.WithIsRetryableErr(cerrors.IsRetryableError))
		if err != nil {
			log.Warn("error refreshing cluster info", zap.Error(err))
		}

		tables, ok := c.captures[sourceCapture]
		if !ok {
			log.Warn("source capture is gone", zap.String("sourceCapture", sourceCapture))
			return errors.New("source capture is gone")
		}

		if len(tables) == 0 {
			log.Info("source capture is now empty", zap.String("sourceCapture", sourceCapture))
			break
		}

		if counter != maxCheckSourceEmptyRetries {
			log.Debug("source capture is not empty, will try again", zap.String("sourceCapture", sourceCapture))
			time.Sleep(time.Second * 10)
		} else {
			return errors.New("source capture is not empty after retries")
		}
	}

	return nil
}

func (c *cluster) refreshInfo(ctx context.Context) error {
	ownerID, err := c.cdcEtcdCli.GetOwnerID(ctx, etcd.CaptureOwnerKey)
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
		changefeed = k.ID
		break
	}

	c.captures = make(map[string][]*tableInfo)
	_, captures, err := c.cdcEtcdCli.GetCaptures(ctx)
	if err != nil {
		return errors.Trace(err)
	}
	for _, capture := range captures {
		c.captures[capture.ID] = make([]*tableInfo, 0)
		processorDetails, err := queryProcessor(c.ownerAddr, changefeed, capture.ID)
		if err != nil {
			return errors.Trace(err)
		}

		log.Debug("retrieved processor details",
			zap.String("changefeed", changefeed),
			zap.String("captureID", capture.ID),
			zap.Any("processorDetail", processorDetails))
		for _, tableID := range processorDetails.Tables {
			c.captures[capture.ID] = append(c.captures[capture.ID], &tableInfo{
				ID:         tableID,
				Changefeed: changefeed,
			})
		}
	}
	return nil
}

// queryProcessor invokes the following API to get the mapping from
// captureIDs to tableIDs:
//
//	GET /api/v1/processors/{changefeed_id}/{capture_id}
func queryProcessor(
	apiEndpoint string,
	changefeed string,
	captureID string,
) (*model.ProcessorDetail, error) {
	httpClient, err := httputil.NewClient(&security.Credential{ /* no TLS */ })
	if err != nil {
		return nil, errors.Trace(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	requestURL := fmt.Sprintf("http://%s/api/v1/processors/%s/%s", apiEndpoint, changefeed, captureID)
	resp, err := httpClient.Get(ctx, requestURL)
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer func() {
		_ = resp.Body.Close()
	}()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, errors.Trace(
			errors.Errorf("HTTP API returned error status: %d, url: %s", resp.StatusCode, requestURL))
	}

	bodyBytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, errors.Trace(err)
	}

	var ret model.ProcessorDetail
	err = json.Unmarshal(bodyBytes, &ret)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &ret, nil
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
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return errors.Trace(err)
		}
		log.Warn("http error", zap.ByteString("body", body))
		return errors.New(resp.Status)
	}

	return nil
}
