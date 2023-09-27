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

package server

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/gin-gonic/gin"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/util/gctuner"
	"github.com/pingcap/tiflow/cdc"
	"github.com/pingcap/tiflow/cdc/capture"
	"github.com/pingcap/tiflow/cdc/contextutil"
	"github.com/pingcap/tiflow/cdc/kv"
	"github.com/pingcap/tiflow/cdc/processor/sourcemanager/engine/factory"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/etcd"
	"github.com/pingcap/tiflow/pkg/fsutil"
	clogutil "github.com/pingcap/tiflow/pkg/logutil"
	"github.com/pingcap/tiflow/pkg/p2p"
	"github.com/pingcap/tiflow/pkg/pdutil"
	"github.com/pingcap/tiflow/pkg/tcpserver"
	p2pProto "github.com/pingcap/tiflow/proto/p2p"
	pd "github.com/tikv/pd/client"
	"go.uber.org/zap"
	"golang.org/x/net/netutil"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
)

const (
	defaultDataDir = "/tmp/cdc_data"
	// dataDirThreshold is used to warn if the free space of the specified data-dir is lower than it, unit is GB
	dataDirThreshold = 500
	// maxHTTPConnection is used to limit the max concurrent connections of http server.
	maxHTTPConnection = 1000
	// httpConnectionTimeout is used to limit a connection max alive time of http server.
	httpConnectionTimeout = 10 * time.Minute
	// maxGcTunerMemory is used to limit the max memory usage of cdc server. if the memory is larger than it, gc tuner will be disabled
	maxGcTunerMemory = 512 * 1024 * 1024 * 1024
)

// Server is the interface for the TiCDC server
type Server interface {
	// Run runs the server.
	Run(ctx context.Context) error
	// Close closes the server.
	Close()
	// Drain removes tables in the current TiCDC instance.
	// It's part of graceful shutdown, should be called before Close.
	Drain() <-chan struct{}
}

// server implement the TiCDC Server interface
// TODO: we need to make server more unit testable and add more test cases.
// Especially we need to decouple the HTTPServer out of server.
type server struct {
	capture      capture.Capture
	tcpServer    tcpserver.TCPServer
	grpcService  *p2p.ServerWrapper
	statusServer *http.Server
	etcdClient   etcd.CDCEtcdClient
	// pdClient is the default upstream PD client.
	// The PD acts as a metadata management service for TiCDC.
	pdClient          pd.Client
	pdAPIClient       pdutil.PDAPIClient
	pdEndpoints       []string
	sortEngineFactory *factory.SortEngineFactory
}

// New creates a server instance.
func New(pdEndpoints []string) (*server, error) {
	conf := config.GetGlobalServerConfig()

	// This is to make communication between nodes possible.
	// In other words, the nodes have to trust each other.
	if len(conf.Security.CertAllowedCN) != 0 {
		err := conf.Security.AddSelfCommonName()
		if err != nil {
			return nil, errors.Trace(err)
		}
	}

	// tcpServer is the unified frontend of the CDC server that serves
	// both RESTful APIs and gRPC APIs.
	// Note that we pass the TLS config to the tcpServer, so there is no need to
	// configure TLS elsewhere.
	tcpServer, err := tcpserver.NewTCPServer(conf.Addr, conf.Security)
	if err != nil {
		return nil, errors.Trace(err)
	}

	debugConfig := config.GetGlobalServerConfig().Debug
	s := &server{
		pdEndpoints: pdEndpoints,
		grpcService: p2p.NewServerWrapper(debugConfig.Messages.ToMessageServerConfig()),
		tcpServer:   tcpServer,
	}

	log.Info("CDC server created",
		zap.Strings("pd", pdEndpoints), zap.Stringer("config", conf))

	return s, nil
}

func (s *server) prepare(ctx context.Context) error {
	conf := config.GetGlobalServerConfig()

	tlsConfig, err := conf.Security.ToTLSConfig()
	if err != nil {
		return errors.Trace(err)
	}
	grpcTLSOption, err := conf.Security.ToGRPCDialOption()
	if err != nil {
		return errors.Trace(err)
	}
	log.Info("create pd client", zap.Strings("endpoints", s.pdEndpoints))
	s.pdClient, err = pd.NewClientWithContext(
		ctx, s.pdEndpoints, conf.Security.PDSecurityOption(),
		// the default `timeout` is 3s, maybe too small if the pd is busy,
		// set to 10s to avoid frequent timeout.
		pd.WithCustomTimeoutOption(10*time.Second),
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
		return errors.Trace(err)
	}
	s.pdAPIClient, err = pdutil.NewPDAPIClient(s.pdClient, conf.Security)
	if err != nil {
		return errors.Trace(err)
	}
	log.Info("create etcdCli", zap.Strings("endpoints", s.pdEndpoints))
	// we do not pass a `context` to create a the etcd client,
	// to prevent it's cancelled when the server is closing.
	// For example, when the non-owner node goes offline,
	// it would resign the campaign key which was put by call `campaign`,
	// if this is not done due to the passed context cancelled,
	// the key will be kept for the lease TTL, which is 10 seconds,
	// then cause the new owner cannot be elected immediately after the old owner offline.
	// see https://github.com/etcd-io/etcd/blob/525d53bd41/client/v3/concurrency/election.go#L98
	etcdCli, err := etcd.CreateRawEtcdClient(tlsConfig, grpcTLSOption, s.pdEndpoints...)
	if err != nil {
		return errors.Trace(err)
	}

	cdcEtcdClient, err := etcd.NewCDCEtcdClient(ctx, etcdCli, conf.ClusterID)
	if err != nil {
		return errors.Trace(err)
	}
	s.etcdClient = cdcEtcdClient

	// Collect all endpoints from pd here to make the server more robust.
	// Because in some scenarios, the deployer may only provide one pd endpoint,
	// this will cause the TiCDC server to fail to restart when some pd node is down.
	allPDEndpoints, err := s.pdAPIClient.CollectMemberEndpoints(ctx)
	if err != nil {
		return errors.Trace(err)
	}
	s.pdEndpoints = append(s.pdEndpoints, allPDEndpoints...)

	err = s.initDir(ctx)
	if err != nil {
		return errors.Trace(err)
	}

	s.createSortEngineFactory()
	s.setMemoryLimit()

	s.capture = capture.NewCapture(s.pdEndpoints, cdcEtcdClient,
		s.grpcService, s.sortEngineFactory, s.pdClient)
	return nil
}

func (s *server) setMemoryLimit() {
	conf := config.GetGlobalServerConfig()
	if conf.GcTunerMemoryThreshold > maxGcTunerMemory {
		// If total memory is larger than 512GB, we will not set memory limit.
		// Because the memory limit is not accurate, and it is not necessary to set memory limit.
		log.Warn("total memory is larger than 512GB, skip setting memory limit",
			zap.Uint64("bytes", conf.GcTunerMemoryThreshold),
			zap.String("memory", humanize.IBytes(conf.GcTunerMemoryThreshold)),
		)
		return
	}
	if conf.GcTunerMemoryThreshold > 0 {
		gctuner.EnableGOGCTuner.Store(true)
		gctuner.Tuning(conf.GcTunerMemoryThreshold)
		log.Info("enable gctuner, set memory limit",
			zap.Uint64("bytes", conf.GcTunerMemoryThreshold),
			zap.String("memory", humanize.IBytes(conf.GcTunerMemoryThreshold)),
		)
	}
}

func (s *server) createSortEngineFactory() {
	conf := config.GetGlobalServerConfig()
	if s.sortEngineFactory != nil {
		if err := s.sortEngineFactory.Close(); err != nil {
			log.Error("fails to close sort engine manager", zap.Error(err))
		}
		s.sortEngineFactory = nil
	}

	// Sorter dir has been set and checked when server starts.
	// See https://github.com/pingcap/tiflow/blob/9dad09/cdc/server.go#L275
	sortDir := config.GetGlobalServerConfig().Sorter.SortDir
	memInBytes := conf.Sorter.CacheSizeInMB * uint64(1<<20)
	s.sortEngineFactory = factory.NewForPebble(sortDir, memInBytes, conf.Debug.DB)
	log.Info("sorter engine memory limit",
		zap.Uint64("bytes", memInBytes),
		zap.String("memory", humanize.IBytes(memInBytes)),
	)
}

// Run runs the server.
func (s *server) Run(serverCtx context.Context) error {
	if err := s.prepare(serverCtx); err != nil {
		return err
	}

	err := s.startStatusHTTP(serverCtx, s.tcpServer.HTTP1Listener())
	if err != nil {
		return err
	}

	return s.run(serverCtx)
}

// startStatusHTTP starts the HTTP server.
// `lis` is a listener that gives us plain-text HTTP requests.
// TODO: can we decouple the HTTP server from the capture server?
func (s *server) startStatusHTTP(serverCtx context.Context, lis net.Listener) error {
	// LimitListener returns a Listener that accepts at most n simultaneous
	// connections from the provided Listener. Connections that exceed the
	// limit will wait in a queue and no new goroutines will be created until
	// a connection is processed.
	// We use it here to limit the max concurrent connections of statusServer.
	lis = netutil.LimitListener(lis, maxHTTPConnection)
	conf := config.GetGlobalServerConfig()

	logWritter := clogutil.InitGinLogWritter()
	router := gin.New()
	// add gin.RecoveryWithWriter() to handle unexpected panic (logging and
	// returning status code 500)
	router.Use(gin.RecoveryWithWriter(logWritter))
	// router.
	// Register APIs.
	cdc.RegisterRoutes(router, s.capture, registry)

	// No need to configure TLS because it is already handled by `s.tcpServer`.
	// Add ReadTimeout and WriteTimeout to avoid some abnormal connections never close.
	s.statusServer = &http.Server{
		Handler:      router,
		ReadTimeout:  httpConnectionTimeout,
		WriteTimeout: httpConnectionTimeout,
		BaseContext: func(listener net.Listener) context.Context {
			return contextutil.PutTimezoneInCtx(context.Background(),
				contextutil.TimezoneFromCtx(serverCtx))
		},
	}

	go func() {
		log.Info("http server is running", zap.String("addr", conf.Addr))
		err := s.statusServer.Serve(lis)
		if err != nil && err != http.ErrServerClosed {
			log.Error("http server error", zap.Error(cerror.WrapError(cerror.ErrServeHTTP, err)))
		}
	}()
	return nil
}

func (s *server) upstreamPDHealthChecker(ctx context.Context) error {
	ticker := time.NewTicker(time.Second * 3)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			endpoints, err := s.pdAPIClient.CollectMemberEndpoints(ctx)
			if err != nil {
				log.Warn("etcd health check: cannot collect all members", zap.Error(err))
				continue
			}
			for _, endpoint := range endpoints {
				start := time.Now()
				ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
				if err := s.pdAPIClient.Healthy(ctx, endpoint); err != nil {
					log.Warn("etcd health check error",
						zap.String("endpoint", endpoint), zap.Error(err))
				}
				etcdHealthCheckDuration.WithLabelValues(endpoint).
					Observe(time.Since(start).Seconds())
				cancel()
			}
			ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
			_, err = s.etcdClient.GetEtcdClient().Unwrap().MemberList(ctx)
			cancel()
			if err != nil {
				log.Warn("etcd health check error, fail to list etcd members", zap.Error(err))
			}
		}
	}
}

func (s *server) run(ctx context.Context) (err error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	defer s.pdAPIClient.Close()

	eg, egCtx := errgroup.WithContext(ctx)

	eg.Go(func() error {
		return s.capture.Run(egCtx)
	})

	eg.Go(func() error {
		return s.upstreamPDHealthChecker(egCtx)
	})

	eg.Go(func() error {
		return kv.RunWorkerPool(egCtx)
	})

	eg.Go(func() error {
		return s.tcpServer.Run(egCtx)
	})

	grpcServer := grpc.NewServer(s.grpcService.ServerOptions()...)
	p2pProto.RegisterCDCPeerToPeerServer(grpcServer, s.grpcService)

	eg.Go(func() error {
		return grpcServer.Serve(s.tcpServer.GrpcListener())
	})
	eg.Go(func() error {
		<-egCtx.Done()
		grpcServer.Stop()
		return nil
	})

	return eg.Wait()
}

// Drain removes tables in the current TiCDC instance.
// It's part of graceful shutdown, should be called before Close.
func (s *server) Drain() <-chan struct{} {
	return s.capture.Drain()
}

// Close closes the server.
func (s *server) Close() {
	s.closeSortEngineFactory()

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
	if s.tcpServer != nil {
		err := s.tcpServer.Close()
		if err != nil {
			log.Error("close tcp server", zap.Error(err))
		}
		s.tcpServer = nil
	}

	if s.pdClient != nil {
		s.pdClient.Close()
	}
}

func (s *server) closeSortEngineFactory() {
	start := time.Now()
	if s.sortEngineFactory != nil {
		if err := s.sortEngineFactory.Close(); err != nil {
			log.Error("fails to close sort engine manager", zap.Error(err))
		}
		log.Info("sort engine manager closed", zap.Duration("duration", time.Since(start)))
	}
}

func (s *server) initDir(ctx context.Context) error {
	if err := s.setUpDir(ctx); err != nil {
		return errors.Trace(err)
	}
	conf := config.GetGlobalServerConfig()
	// Ensure data dir exists and read-writable.
	diskInfo, err := checkDir(conf.DataDir)
	if err != nil {
		return errors.Trace(err)
	}
	log.Info(fmt.Sprintf("%s is set as data-dir (%dGB available), sort-dir=%s. "+
		"It is recommended that the disk for data-dir at least have %dGB available space",
		conf.DataDir, diskInfo.Avail, conf.Sorter.SortDir, dataDirThreshold))

	// Ensure sorter dir exists and read-writable.
	_, err = checkDir(conf.Sorter.SortDir)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (s *server) setUpDir(ctx context.Context) error {
	conf := config.GetGlobalServerConfig()
	if conf.DataDir != "" {
		conf.Sorter.SortDir = filepath.Join(conf.DataDir, config.DefaultSortDir)
		config.StoreGlobalServerConfig(conf)

		return nil
	}

	// data-dir will be decided by exist changefeed for backward compatibility
	allInfo, err := s.etcdClient.GetAllChangeFeedInfo(ctx)
	if err != nil {
		return errors.Trace(err)
	}

	candidates := make([]string, 0, len(allInfo))
	for _, info := range allInfo {
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

func checkDir(dir string) (*fsutil.DiskInfo, error) {
	err := os.MkdirAll(dir, 0o700)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if err := fsutil.IsDirReadWritable(dir); err != nil {
		return nil, errors.Trace(err)
	}
	return fsutil.GetDiskInfo(dir)
}

// try to find the best data dir by rules
// at the moment, only consider available disk space
func findBestDataDir(candidates []string) (result string, ok bool) {
	var low uint64 = 0

	for _, dir := range candidates {
		info, err := checkDir(dir)
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
