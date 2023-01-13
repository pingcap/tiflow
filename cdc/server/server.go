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
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/util/memory"
	"github.com/pingcap/tiflow/cdc"
	"github.com/pingcap/tiflow/cdc/capture"
	"github.com/pingcap/tiflow/cdc/kv"
	"github.com/pingcap/tiflow/cdc/processor/pipeline/system"
	"github.com/pingcap/tiflow/cdc/processor/sourcemanager/engine/factory"
	ssystem "github.com/pingcap/tiflow/cdc/sorter/db/system"
	"github.com/pingcap/tiflow/cdc/sorter/unified"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/etcd"
	"github.com/pingcap/tiflow/pkg/fsutil"
	"github.com/pingcap/tiflow/pkg/p2p"
	"github.com/pingcap/tiflow/pkg/pdutil"
	"github.com/pingcap/tiflow/pkg/tcpserver"
	p2pProto "github.com/pingcap/tiflow/proto/p2p"
	pd "github.com/tikv/pd/client"
	"go.etcd.io/etcd/client/pkg/v3/logutil"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
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
	pdEndpoints  []string

	tableActorSystem *system.System

	// If it's true sortEngineManager will be used, otherwise sorterSystem will be used.
	useEventSortEngine bool
	sortEngineFactory  *factory.SortEngineFactory
	sorterSystem       *ssystem.System
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

	// TODO(qupeng): adjust it after all sorters are transformed into EventSortEngine.
	debugConfig := config.GetGlobalServerConfig().Debug
	useEventSortEngine := debugConfig.EnablePullBasedSink && debugConfig.EnableDBSorter

	s := &server{
		pdEndpoints: pdEndpoints,
		grpcService: p2p.NewServerWrapper(debugConfig.Messages.ToMessageServerConfig()),
		tcpServer:   tcpServer,

		useEventSortEngine: useEventSortEngine,
	}

	log.Info("CDC server created",
		zap.Strings("pd", pdEndpoints), zap.Stringer("config", conf))

	return s, nil
}

func (s *server) prepare(ctx context.Context) error {
	conf := config.GetGlobalServerConfig()

	grpcTLSOption, err := conf.Security.ToGRPCDialOption()
	if err != nil {
		return errors.Trace(err)
	}

	tlsConfig, err := conf.Security.ToTLSConfig()
	if err != nil {
		return errors.Trace(err)
	}

	logConfig := logutil.DefaultZapLoggerConfig
	logConfig.Level = zap.NewAtomicLevelAt(zapcore.ErrorLevel)

	createEtcdClient := func() (etcd.CDCEtcdClient, error) {
		etcdCli, err := clientv3.New(clientv3.Config{
			Endpoints:   s.pdEndpoints,
			TLS:         tlsConfig,
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
			return nil, cerror.WrapError(cerror.ErrNewCaptureFailed, err)
		}
		cdcEtcdClient, err := etcd.NewCDCEtcdClient(ctx, etcdCli, conf.ClusterID)
		if err != nil {
			etcdCli.Close()
			return nil, cerror.WrapError(cerror.ErrNewCaptureFailed, err)
		}
		return cdcEtcdClient, nil
	}

	cdcEtcdClient, err := createEtcdClient()
	if err != nil {
		return cerror.WrapError(cerror.ErrNewCaptureFailed, err)
	}

	s.etcdClient = cdcEtcdClient

	err = s.initDir(ctx)
	if err != nil {
		return errors.Trace(err)
	}

	if err := s.startActorSystems(ctx); err != nil {
		return errors.Trace(err)
	}

	s.capture = capture.NewCapture(
		s.pdEndpoints, createEtcdClient, s.grpcService,
		s.tableActorSystem, s.sortEngineFactory, s.sorterSystem)

	return nil
}

func (s *server) startActorSystems(ctx context.Context) error {
	if s.tableActorSystem != nil {
		s.tableActorSystem.Stop()
	}
	s.tableActorSystem = system.NewSystem()
	s.tableActorSystem.Start(ctx)

	conf := config.GetGlobalServerConfig()
	if !conf.Debug.EnableDBSorter {
		return nil
	}

	if s.useEventSortEngine && s.sortEngineFactory != nil {
		if err := s.sortEngineFactory.Close(); err != nil {
			log.Error("fails to close sort engine manager", zap.Error(err))
		}
		s.sortEngineFactory = nil
	}
	if !s.useEventSortEngine && s.sorterSystem != nil {
		s.sorterSystem.Stop()
	}

	// Sorter dir has been set and checked when server starts.
	// See https://github.com/pingcap/tiflow/blob/9dad09/cdc/server.go#L275
	sortDir := config.GetGlobalServerConfig().Sorter.SortDir

	if s.useEventSortEngine {
		totalMemory, err := memory.MemTotal()
		if err != nil {
			return errors.Trace(err)
		}
		memPercentage := float64(conf.Sorter.MaxMemoryPercentage) / 100
		memInBytes := uint64(float64(totalMemory) * memPercentage)
		if config.GetGlobalServerConfig().Debug.EnableDBSorter {
			s.sortEngineFactory = factory.NewForPebble(sortDir, memInBytes, conf.Debug.DB)
		} else {
			panic("only pebble is transformed to EventSortEngine")
		}
	} else {
		memPercentage := float64(conf.Sorter.MaxMemoryPercentage) / 100
		s.sorterSystem = ssystem.NewSystem(sortDir, memPercentage, conf.Debug.DB)
		err := s.sorterSystem.Start(ctx)
		if err != nil {
			return errors.Trace(err)
		}
	}

	return nil
}

// Run runs the server.
func (s *server) Run(ctx context.Context) error {
	if err := s.prepare(ctx); err != nil {
		return err
	}

	err := s.startStatusHTTP(s.tcpServer.HTTP1Listener())
	if err != nil {
		return err
	}

	return s.run(ctx)
}

// startStatusHTTP starts the HTTP server.
// `lis` is a listener that gives us plain-text HTTP requests.
// TODO: can we decouple the HTTP server from the capture server?
func (s *server) startStatusHTTP(lis net.Listener) error {
	// LimitListener returns a Listener that accepts at most n simultaneous
	// connections from the provided Listener. Connections that exceed the
	// limit will wait in a queue and no new goroutines will be created until
	// a connection is processed.
	// We use it here to limit the max concurrent connections of statusServer.
	lis = netutil.LimitListener(lis, maxHTTPConnection)
	conf := config.GetGlobalServerConfig()

	// discard gin log output
	gin.DefaultWriter = io.Discard
	router := gin.New()
	// add gin.Recovery() to handle unexpected panic
	router.Use(gin.Recovery())
	// router.
	// Register APIs.
	cdc.RegisterRoutes(router, s.capture, registry)

	// No need to configure TLS because it is already handled by `s.tcpServer`.
	// Add ReadTimeout and WriteTimeout to avoid some abnormal connections never close.
	s.statusServer = &http.Server{
		Handler:      router,
		ReadTimeout:  httpConnectionTimeout,
		WriteTimeout: httpConnectionTimeout,
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

func (s *server) etcdHealthChecker(ctx context.Context) error {
	ticker := time.NewTicker(time.Second * 3)
	defer ticker.Stop()

	conf := config.GetGlobalServerConfig()
	grpcClient, err := pd.NewClientWithContext(ctx, s.pdEndpoints, conf.Security.PDSecurityOption())
	if err != nil {
		return errors.Trace(err)
	}
	pc, err := pdutil.NewPDAPIClient(grpcClient, conf.Security)
	if err != nil {
		return errors.Trace(err)
	}
	defer pc.Close()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			endpoints, err := pc.CollectMemberEndpoints(ctx)
			if err != nil {
				log.Warn("etcd health check: cannot collect all members", zap.Error(err))
				continue
			}
			for _, endpoint := range endpoints {
				start := time.Now()
				ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
				if err := pc.Healthy(ctx, endpoint); err != nil {
					log.Warn("etcd health check error",
						zap.String("endpoint", endpoint), zap.Error(err))
				}
				etcdHealthCheckDuration.WithLabelValues(endpoint).
					Observe(time.Since(start).Seconds())
				cancel()
			}
		}
	}
}

func (s *server) run(ctx context.Context) (err error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	wg, cctx := errgroup.WithContext(ctx)

	wg.Go(func() error {
		return s.capture.Run(cctx)
	})

	wg.Go(func() error {
		return s.etcdHealthChecker(cctx)
	})

	wg.Go(func() error {
		return kv.RunWorkerPool(cctx)
	})

	wg.Go(func() error {
		return s.tcpServer.Run(cctx)
	})

	conf := config.GetGlobalServerConfig()

	if !conf.Debug.EnableDBSorter {
		wg.Go(func() error {
			return unified.RunWorkerPool(cctx)
		})
	}

	if conf.Debug.EnableNewScheduler {
		grpcServer := grpc.NewServer(s.grpcService.ServerOptions()...)
		p2pProto.RegisterCDCPeerToPeerServer(grpcServer, s.grpcService)

		wg.Go(func() error {
			return grpcServer.Serve(s.tcpServer.GrpcListener())
		})
		wg.Go(func() error {
			<-cctx.Done()
			grpcServer.Stop()
			return nil
		})
	}

	return wg.Wait()
}

// Drain removes tables in the current TiCDC instance.
// It's part of graceful shutdown, should be called before Close.
func (s *server) Drain() <-chan struct{} {
	return s.capture.Drain()
}

// Close closes the server.
func (s *server) Close() {
	s.stopActorSystems()

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
}

func (s *server) stopActorSystems() {
	start := time.Now()
	if s.tableActorSystem != nil {
		s.tableActorSystem.Stop()
		s.tableActorSystem = nil
	}
	log.Info("table actor system closed", zap.Duration("duration", time.Since(start)))

	start = time.Now()
	if s.useEventSortEngine && s.sortEngineFactory != nil {
		if err := s.sortEngineFactory.Close(); err != nil {
			log.Error("fails to close sort engine manager", zap.Error(err))
		}
		log.Info("sort engine manager closed", zap.Duration("duration", time.Since(start)))
	}
	if !s.useEventSortEngine && s.sorterSystem != nil {
		s.sorterSystem.Stop()
		s.sorterSystem = nil
		log.Info("sorter actor system closed", zap.Duration("duration", time.Since(start)))
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
