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
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

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
	pd "github.com/tikv/pd/client"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/mvcc"
	"go.etcd.io/etcd/pkg/logutil"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"golang.org/x/sync/errgroup"
	"golang.org/x/time/rate"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
)

const (
	ownerRunInterval = time.Millisecond * 500
	defaultDataDir   = "/tmp/cdc_data"
	// dataDirThreshold is used to warn if the free space of the specified data-dir is lower than it, unit is GB
	dataDirThreshold = 500
)

// Server is the capture server
type Server struct {
	captureV2 *capture.Capture

	capture      *Capture
	owner        *Owner
	ownerLock    sync.RWMutex
	statusServer *http.Server
	pdClient     pd.Client
	etcdClient   *kv.CDCEtcdClient
	kvStorage    tidbkv.Storage
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
	if config.NewReplicaImpl {
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
		etcdClient := kv.NewCDCEtcdClient(ctx, etcdCli)
		s.etcdClient = &etcdClient
	}

	if err := s.initDataDir(ctx); err != nil {
		return errors.Trace(err)
	}
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
	if config.NewReplicaImpl {
		s.captureV2 = capture.NewCapture(s.pdClient, s.kvStorage, s.etcdClient)
		return s.run(ctx)
	}
	// When a capture suicided, restart it
	for {
		if err := s.run(ctx); cerror.ErrCaptureSuicide.NotEqual(err) {
			return err
		}
		log.Info("server recovered", zap.String("capture-id", s.capture.info.ID))
	}
}

func (s *Server) setOwner(owner *Owner) {
	s.ownerLock.Lock()
	defer s.ownerLock.Unlock()
	s.owner = owner
}

func (s *Server) campaignOwnerLoop(ctx context.Context) error {
	// In most failure cases, we don't return error directly, just run another
	// campaign loop. We treat campaign loop as a special background routine.

	conf := config.GetGlobalServerConfig()
	rl := rate.NewLimiter(0.05, 2)
	for {
		err := rl.Wait(ctx)
		if err != nil {
			if errors.Cause(err) == context.Canceled {
				return nil
			}
			return errors.Trace(err)
		}

		// Campaign to be an owner, it blocks until it becomes the owner
		if err := s.capture.Campaign(ctx); err != nil {
			switch errors.Cause(err) {
			case context.Canceled:
				return nil
			case mvcc.ErrCompacted:
				continue
			}
			log.Warn("campaign owner failed", zap.Error(err))
			continue
		}
		captureID := s.capture.info.ID
		log.Info("campaign owner successfully", zap.String("capture-id", captureID))
		owner, err := NewOwner(ctx, s.pdClient, conf.Security, s.capture.session, conf.GcTTL, time.Duration(conf.OwnerFlushInterval))
		if err != nil {
			log.Warn("create new owner failed", zap.Error(err))
			continue
		}

		s.setOwner(owner)
		if err := owner.Run(ctx, ownerRunInterval); err != nil {
			if errors.Cause(err) == context.Canceled {
				log.Info("owner exited", zap.String("capture-id", captureID))
				select {
				case <-ctx.Done():
					// only exits the campaignOwnerLoop if parent context is done
					return ctx.Err()
				default:
				}
				log.Info("owner exited", zap.String("capture-id", captureID))
			}
			err2 := s.capture.Resign(ctx)
			if err2 != nil {
				// if resign owner failed, return error to let capture exits
				return errors.Annotatef(err2, "resign owner failed, capture: %s", captureID)
			}
			log.Warn("run owner failed", zap.Error(err))
		}
		// owner is resigned by API, reset owner and continue the campaign loop
		s.setOwner(nil)
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
	if !config.NewReplicaImpl {
		kvStorage, err := util.KVStorageFromCtx(ctx)
		if err != nil {
			return errors.Trace(err)
		}
		capture, err := NewCapture(ctx, s.pdEndpoints, s.pdClient, kvStorage)
		if err != nil {
			return err
		}
		s.capture = capture
		s.etcdClient = &capture.etcdClient
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	wg, cctx := errgroup.WithContext(ctx)
	if config.NewReplicaImpl {
		wg.Go(func() error {
			return s.captureV2.Run(cctx)
		})
	} else {
		wg.Go(func() error {
			return s.campaignOwnerLoop(cctx)
		})

		wg.Go(func() error {
			return s.capture.Run(cctx)
		})
	}
	wg.Go(func() error {
		return s.etcdHealthChecker(cctx)
	})

	wg.Go(func() error {
		return sorter.RunWorkerPool(cctx)
	})

	wg.Go(func() error {
		return kv.RunWorkerPool(cctx)
	})

	return wg.Wait()
}

// Close closes the server.
func (s *Server) Close() {
	if s.capture != nil {
		if !config.NewReplicaImpl {
			s.capture.Cleanup()
		}
		closeCtx, closeCancel := context.WithTimeout(context.Background(), time.Second*2)
		err := s.capture.Close(closeCtx)
		if err != nil {
			log.Error("close capture", zap.Error(err))
		}
		closeCancel()
	}
	if s.captureV2 != nil {
		s.captureV2.AsyncClose()
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

	if diskInfo.Avail < dataDirThreshold {
		log.Warn(fmt.Sprintf("%s is set as data-dir (%dGB available), ticdc recommend disk for data-dir "+
			"at least have %dGB available space", conf.DataDir, diskInfo.Avail, dataDirThreshold))
	}

	return nil
}

func (s *Server) setUpDataDir(ctx context.Context) error {
	conf := config.GetGlobalServerConfig()
	if conf.DataDir != "" {
		conf.Sorter.SortDir = filepath.Join(conf.DataDir, config.DefaultSortDir)
		config.StoreGlobalServerConfig(conf)

		return nil
	}

	// s.etcdClient maybe nil if NewReplicaImpl is not set to true
	// todo: remove this after NewReplicaImpl set to true in a specific branch, and use server.etcdClient instead.
	cli := s.etcdClient
	if cli == nil {
		client, err := clientv3.New(clientv3.Config{
			Endpoints:   s.pdEndpoints,
			Context:     ctx,
			DialTimeout: 5 * time.Second,
		})
		if err != nil {
			return err
		}
		etcdClient := kv.NewCDCEtcdClient(ctx, client)
		cli = &etcdClient
		defer cli.Close()
	}

	// data-dir will be decide by exist changefeed for backward compatibility
	allStatus, err := cli.GetAllChangeFeedStatus(ctx)
	if err != nil {
		return errors.Trace(err)
	}

	candidates := make([]string, 0, len(allStatus))
	for id := range allStatus {
		info, err := cli.GetChangeFeedInfo(ctx, id)
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
	for _, dir := range candidates {
		if err := util.IsDirReadWritable(dir); err != nil {
			log.Warn("try to get disk info failed", zap.String("dir", dir), zap.Error(err))
			continue
		}
		info, err := util.GetDiskInfo(dir)
		if err != nil {
			log.Warn("try to get disk info failed", zap.String("dir", dir), zap.Error(err))
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
