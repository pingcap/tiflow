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

package cdc

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	"go.etcd.io/etcd/pkg/logutil"
	"go.uber.org/zap/zapcore"

	"github.com/pingcap/ticdc/cdc/capture"
	"go.etcd.io/etcd/clientv3"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cdc/kv"
	"github.com/pingcap/ticdc/cdc/puller/sorter"
	"github.com/pingcap/ticdc/pkg/config"
	cerror "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/httputil"
	"github.com/pingcap/ticdc/pkg/util"
	"github.com/pingcap/ticdc/pkg/version"
	"github.com/prometheus/client_golang/prometheus"
	pd "github.com/tikv/pd/client"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
)

const (
	ownerRunInterval = time.Millisecond * 500
)

// Server is the capture server
type Server struct {
	capture *capture.Capture

	statusServer *http.Server
	pdClient     pd.Client
	etcdClient   *clientv3.Client
	pdEndpoints  []string
}

// NewServer creates a Server instance.
func NewServer(pdEndpoints []string) (*Server, error) {
	conf := config.GetGlobalServerConfig()
	log.Info("creating CDC server",
		zap.Strings("pd-addrs", pdEndpoints),
		zap.Stringer("config", conf),
	)

	s := &Server{
		pdEndpoints: pdEndpoints,
	}
	return s, nil
}

// Run runs the server.
func (s *Server) Run(ctx context.Context) error {
	conf := config.GetGlobalServerConfig()

	grpcTLSOption, err := conf.Security.ToGRPCDialOption()
	if err != nil {
		return errors.Trace(err)
	}
	pdClient, err := pd.NewClientWithContext(
		ctx, s.pdEndpoints, conf.Security.PDSecurityOption(),
		pd.WithGRPCDialOptions(
			grpcTLSOption,
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
		))
	if err != nil {
		return cerror.WrapError(cerror.ErrServerNewPDClient, err)
	}
	s.pdClient = pdClient

	tlsConfig, err := conf.Security.ToTLSConfig()
	if err != nil {
		return errors.Trace(err)
	}
	logConfig := logutil.DefaultZapLoggerConfig
	logConfig.Level = zap.NewAtomicLevelAt(zapcore.ErrorLevel)
	etcdCli, err := clientv3.New(clientv3.Config{
		Endpoints:   s.pdEndpoints,
		TLS:         tlsConfig,
		Context:     ctx,
		LogConfig:   &logConfig,
		DialTimeout: 5 * time.Second,
		DialOptions: []grpc.DialOption{
			grpcTLSOption,
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
		return errors.Annotate(cerror.WrapError(cerror.ErrNewCaptureFailed, err), "new etcd client")
	}
	s.etcdClient = etcdCli

	// To not block CDC server startup, we need to warn instead of error
	// when TiKV is incompatible.
	errorTiKVIncompatible := false
	err = version.CheckClusterVersion(ctx, s.pdClient, s.pdEndpoints[0], conf.Security, errorTiKVIncompatible)
	if err != nil {
		return err
	}
	err = s.startStatusHTTP()
	if err != nil {
		return err
	}

	kvStore, err := kv.CreateTiStore(strings.Join(s.pdEndpoints, ","), conf.Security)
	if err != nil {
		return errors.Trace(err)
	}
	defer func() {
		err := kvStore.Close()
		if err != nil {
			log.Warn("kv store close failed", zap.Error(err))
		}
	}()
	ctx = util.PutKVStorageInCtx(ctx, kvStore)
	// When a capture suicided, restart it
	for {
		if err := s.run(ctx); cerror.ErrCaptureSuicide.NotEqual(err) {
			return err
		}
		log.Info("server recovered", zap.String("capture-id", s.capture.Info().ID))
	}
}

func (s *Server) etcdHealthChecker(ctx context.Context) error {
	ticker := time.NewTicker(time.Second * 3)
	defer ticker.Stop()
	conf := config.GetGlobalServerConfig()

	httpCli, err := httputil.NewClient(conf.Security)
	if err != nil {
		return err
	}
	defer httpCli.CloseIdleConnections()
	metrics := make(map[string]prometheus.Observer)
	for _, pdEndpoint := range s.pdEndpoints {
		metrics[pdEndpoint] = etcdHealthCheckDuration.WithLabelValues(conf.AdvertiseAddr, pdEndpoint)
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			for _, pdEndpoint := range s.pdEndpoints {
				start := time.Now()
				ctx, cancel := context.WithTimeout(ctx, time.Duration(time.Second*10))
				req, err := http.NewRequestWithContext(
					ctx, http.MethodGet, fmt.Sprintf("%s/health", pdEndpoint), nil)
				if err != nil {
					log.Warn("etcd health check failed", zap.Error(err))
					cancel()
					continue
				}
				_, err = httpCli.Do(req)
				if err != nil {
					log.Warn("etcd health check error", zap.Error(err))
				} else {
					metrics[pdEndpoint].Observe(float64(time.Since(start)) / float64(time.Second))
				}
				cancel()
			}
		}
	}
}

func (s *Server) run(ctx context.Context) (err error) {
	capture, err := capture.NewCapture(ctx, s.etcdClient, s.pdClient)
	if err != nil {
		return err
	}
	s.capture = capture
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	wg, cctx := errgroup.WithContext(ctx)

	wg.Go(func() error {
		return s.etcdHealthChecker(cctx)
	})

	wg.Go(func() error {
		return sorter.RunWorkerPool(cctx)
	})

	wg.Go(func() error {
		return s.capture.Run(cctx)
	})

	return wg.Wait()
}

// Close closes the server.
func (s *Server) Close() {
	if s.capture != nil {
		s.capture.AsyncClose()
	}
	if s.statusServer != nil {
		err := s.statusServer.Close()
		if err != nil {
			log.Error("close status server", zap.Error(err))
		}
		s.statusServer = nil
	}
}
