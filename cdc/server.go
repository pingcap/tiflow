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
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/pingcap/ticdc/pkg/p2p"
	p2p_pb "github.com/pingcap/ticdc/proto/p2p"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cdc/capture"
	"github.com/pingcap/ticdc/cdc/kv"
	"github.com/pingcap/ticdc/cdc/puller/sorter"
	"github.com/pingcap/ticdc/pkg/config"
	cerror "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/httputil"
	"github.com/pingcap/ticdc/pkg/util"
	"github.com/pingcap/ticdc/pkg/version"
	tidbkv "github.com/pingcap/tidb/kv"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/soheilhy/cmux"
	pd "github.com/tikv/pd/client"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/pkg/logutil"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
)

const (
	defaultDataDir = "/tmp/cdc_data"
	// dataDirThreshold is used to warn if the free space of the specified data-dir is lower than it, unit is GB
	dataDirThreshold = 500
)

// Server is the capture server
type Server struct {
	capture      *capture.Capture
	statusServer *http.Server
	pdClient     pd.Client
	etcdClient   *kv.CDCEtcdClient
	kvStorage    tidbkv.Storage
	pdEndpoints  []string

	// TODO remove these by refactoring Run
	grpcListener net.Listener
	tcpListener  net.Listener
	mux          cmux.CMux
	grpcServer   *p2p.ResettableServer
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
		grpcServer:  p2p.NewResettableServer(),
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

	cdcEtcdClient := kv.NewCDCEtcdClient(ctx, etcdCli)
	s.etcdClient = &cdcEtcdClient

	err = s.initDataDir(ctx)
	if err != nil {
		return errors.Trace(err)
	}

	// To not block CDC server startup, we need to warn instead of error
	// when TiKV is incompatible.
	errorTiKVIncompatible := false
	for _, pdEndpoint := range s.pdEndpoints {
		err = version.CheckClusterVersion(ctx, s.pdClient, pdEndpoint, conf.Security, errorTiKVIncompatible)
		if err == nil {
			break
		}
	}
	if err != nil {
		return err
	}

	kv.InitWorkerPool()
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
	s.kvStorage = kvStore
	ctx = util.PutKVStorageInCtx(ctx, kvStore)

	s.tcpListener, err = net.Listen("tcp", conf.Addr)
	if err != nil {
		return errors.Trace(err)
	}

	// TODO refactor the use of listeners and the management of the server goroutines.
	s.mux = cmux.New(s.tcpListener)
	s.grpcListener = s.mux.Match(cmux.HTTP2HeaderField("content-type", "application/grpc"))
	httpLn := s.mux.Match(cmux.Any())

	s.capture = capture.NewCapture(s.pdClient, s.kvStorage, s.etcdClient, s.grpcServer)

	err = s.startStatusHTTP(httpLn)
	if err != nil {
		return errors.Trace(err)
	}

	return s.run(ctx)
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
				ctx, cancel := context.WithTimeout(ctx, time.Second*10)
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
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	wg, cctx := errgroup.WithContext(ctx)

	server := grpc.NewServer()
	p2p_pb.RegisterCDCPeerToPeerServer(server, s.grpcServer)

	wg.Go(func() error {
		return s.capture.Run(cctx)
	})

	wg.Go(func() error {
		return s.etcdHealthChecker(cctx)
	})

	wg.Go(func() error {
		return sorter.RunWorkerPool(cctx)
	})

	wg.Go(func() error {
		return kv.RunWorkerPool(cctx)
	})

	wg.Go(func() error {
		defer log.Info("mux has exited")
		return errors.Trace(s.mux.Serve())
	})

	wg.Go(func() error {
		<-cctx.Done()
		server.Stop()
		return errors.Trace(s.tcpListener.Close())
	})

	wg.Go(func() error {
		return errors.Trace(server.Serve(s.grpcListener))
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

func (s *Server) initDataDir(ctx context.Context) error {
	if err := s.setUpDataDir(ctx); err != nil {
		return errors.Trace(err)
	}
	conf := config.GetGlobalServerConfig()
	err := os.MkdirAll(conf.DataDir, 0o755)
	if err != nil {
		return errors.Trace(err)
	}
	diskInfo, err := util.GetDiskInfo(conf.DataDir)
	if err != nil {
		return errors.Trace(err)
	}

	log.Info(fmt.Sprintf("%s is set as data-dir (%dGB available), sort-dir=%s. "+
		"It is recommended that the disk for data-dir at least have %dGB available space", conf.DataDir, diskInfo.Avail, conf.Sorter.SortDir, dataDirThreshold))

	return nil
}

func (s *Server) setUpDataDir(ctx context.Context) error {
	conf := config.GetGlobalServerConfig()
	if conf.DataDir != "" {
		conf.Sorter.SortDir = filepath.Join(conf.DataDir, config.DefaultSortDir)
		config.StoreGlobalServerConfig(conf)

		return nil
	}

	// data-dir will be decide by exist changefeed for backward compatibility
	allStatus, err := s.etcdClient.GetAllChangeFeedStatus(ctx)
	if err != nil {
		return errors.Trace(err)
	}

	candidates := make([]string, 0, len(allStatus))
	for id := range allStatus {
		info, err := s.etcdClient.GetChangeFeedInfo(ctx, id)
		if err != nil {
			return errors.Trace(err)
		}
		if info.SortDir != "" {
			candidates = append(candidates, info.SortDir)
		}
	}

	conf.DataDir = defaultDataDir
	best, ok := findBestDataDir(candidates)
	if ok {
		conf.DataDir = best
	}

	conf.Sorter.SortDir = filepath.Join(conf.DataDir, config.DefaultSortDir)
	config.StoreGlobalServerConfig(conf)
	return nil
}

// try to find the best data dir by rules
// at the moment, only consider available disk space
func findBestDataDir(candidates []string) (result string, ok bool) {
	var low uint64 = 0

	checker := func(dir string) (*util.DiskInfo, error) {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, err
		}
		if err := util.IsDirReadWritable(dir); err != nil {
			return nil, err
		}
		info, err := util.GetDiskInfo(dir)
		if err != nil {
			return nil, err
		}
		return info, err
	}

	for _, dir := range candidates {
		info, err := checker(dir)
		if err != nil {
			log.Warn("check the availability of dir", zap.String("dir", dir), zap.Error(err))
			continue
		}
		if info.Avail > low {
			result = dir
			low = info.Avail
			ok = true
		}
	}

	if !ok && len(candidates) != 0 {
		log.Warn("try to find directory for data-dir failed, use `/tmp/cdc_data` as data-dir", zap.Strings("candidates", candidates))
	}

	return result, ok
}
