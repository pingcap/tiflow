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

package worker

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/pingcap/errors"
	toolutils "github.com/pingcap/tidb-tools/pkg/utils"
	"github.com/pingcap/tiflow/dm/common"
	"github.com/pingcap/tiflow/dm/config"
	"github.com/pingcap/tiflow/dm/pb"
	"github.com/pingcap/tiflow/dm/pkg/binlog"
	tcontext "github.com/pingcap/tiflow/dm/pkg/context"
	"github.com/pingcap/tiflow/dm/pkg/etcdutil"
	"github.com/pingcap/tiflow/dm/pkg/ha"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/dm/pkg/terror"
	"github.com/pingcap/tiflow/dm/pkg/utils"
	"github.com/pingcap/tiflow/dm/syncer"
	"github.com/pingcap/tiflow/dm/unit"
	"github.com/soheilhy/cmux"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

var (
	cmuxReadTimeout           = 10 * time.Second
	dialTimeout               = 3 * time.Second
	keepaliveTimeout          = 3 * time.Second
	keepaliveTime             = 3 * time.Second
	retryGetSourceBoundConfig = 5
	retryGetRelayConfig       = 5
	retryConnectSleepTime     = time.Second
	syncMasterEndpointsTime   = 3 * time.Second
	getMinLocForSubTaskFunc   = getMinLocForSubTask
)

// Server accepts RPC requests
// dispatches requests to worker
// sends responses to RPC client.
type Server struct {
	// closeMu is used to sync Start/Close and protect 5 fields below
	closeMu sync.Mutex
	// closed is used to indicate whether dm-worker server is in closed state.
	closed atomic.Bool
	// calledClose is used to indicate that dm-worker has received signal to close and closed successfully.
	// we use this variable to avoid Start() after Close()
	calledClose bool
	rootLis     net.Listener
	svr         *grpc.Server
	etcdClient  *clientv3.Client
	// end of closeMu

	wg     sync.WaitGroup
	kaWg   sync.WaitGroup
	httpWg sync.WaitGroup
	runWg  sync.WaitGroup

	ctx    context.Context
	cancel context.CancelFunc

	runCtx    context.Context
	runCancel context.CancelFunc

	kaCtx    context.Context
	kaCancel context.CancelFunc

	cfg *Config

	// mu is used to protect worker and sourceStatus. closeMu should be locked first to avoid
	// deadlock when closeMu and mu are both acquired.
	mu     sync.Mutex
	worker *SourceWorker
	// relay status will never be put in server.sourceStatus
	sourceStatus pb.SourceStatus
}

// NewServer creates a new Server.
func NewServer(cfg *Config) *Server {
	s := Server{
		cfg: cfg,
	}
	s.ctx, s.cancel = context.WithCancel(context.Background())
	s.closed.Store(true) // not start yet
	return &s
}

// Start starts to serving.
// this function should only exit when can't dail DM-master, for other errors it should not exit.
func (s *Server) Start() error {
	log.L().Info("starting dm-worker server")
	RegistryMetrics()

	var m cmux.CMux

	s.runCtx, s.runCancel = context.WithCancel(s.ctx)

	// protect member from data race. some functions below like GetRelayConfig,
	// GetSourceBoundConfig has a built-in timeout so it will not be stuck for a
	// long time.
	startErr := func() error {
		s.closeMu.Lock()
		defer s.closeMu.Unlock()
		// if dm-worker has received signal and finished close, start() should not continue
		if s.calledClose {
			return terror.ErrWorkerServerClosed
		}

		tls, err := toolutils.NewTLS(s.cfg.SSLCA, s.cfg.SSLCert, s.cfg.SSLKey, s.cfg.AdvertiseAddr, s.cfg.CertAllowedCN)
		if err != nil {
			return terror.ErrWorkerTLSConfigNotValid.Delegate(err)
		}

		rootLis, err := net.Listen("tcp", s.cfg.WorkerAddr)
		if err != nil {
			return terror.ErrWorkerStartService.Delegate(err)
		}
		s.rootLis = tls.WrapListener(rootLis)

		s.etcdClient, err = clientv3.New(clientv3.Config{
			Endpoints:            GetJoinURLs(s.cfg.Join),
			DialTimeout:          dialTimeout,
			DialKeepAliveTime:    keepaliveTime,
			DialKeepAliveTimeout: keepaliveTimeout,
			TLS:                  tls.TLSConfig(),
			AutoSyncInterval:     syncMasterEndpointsTime,
		})
		if err != nil {
			return err
		}

		s.setWorker(nil, true)

		s.runWg.Add(1)
		go func() {
			s.runBackgroundJob(s.runCtx)
			s.runWg.Done()
		}()

		s.startKeepAlive()

		relaySource, revRelay, err := ha.GetRelayConfig(s.etcdClient, s.cfg.Name)
		if err != nil {
			return err
		}
		if relaySource != nil {
			log.L().Warn("worker has been assigned relay before keepalive", zap.String("relay source", relaySource.SourceID))
			if err2 := s.enableRelay(relaySource, true); err2 != nil {
				return err2
			}
		}

		s.runWg.Add(1)
		go func(ctx context.Context) {
			defer s.runWg.Done()
			// TODO: handle fatal error from observeRelayConfig
			//nolint:errcheck
			s.observeRelayConfig(ctx, revRelay)
		}(s.runCtx)

		bound, sourceCfg, revBound, err := ha.GetSourceBoundConfig(s.etcdClient, s.cfg.Name)
		if err != nil {
			return err
		}
		if !bound.IsEmpty() {
			log.L().Warn("worker has been assigned source before keepalive", zap.Stringer("bound", bound), zap.Bool("is deleted", bound.IsDeleted))
			if err2 := s.enableHandleSubtasks(sourceCfg, true); err2 != nil {
				return err2
			}
			log.L().Info("started to handle mysql source", zap.String("sourceCfg", sourceCfg.String()))
		}

		s.runWg.Add(1)
		go func(ctx context.Context) {
			defer s.runWg.Done()
			for {
				err1 := s.observeSourceBound(ctx, revBound)
				if err1 == nil {
					return
				}
				s.restartKeepAlive()
			}
		}(s.runCtx)

		// create a cmux
		m = cmux.New(s.rootLis)

		m.SetReadTimeout(cmuxReadTimeout) // set a timeout, ref: https://github.com/pingcap/tidb-binlog/pull/352

		// match connections in order: first gRPC, then HTTP
		grpcL := m.MatchWithWriters(cmux.HTTP2MatchHeaderFieldSendSettings("content-type", "application/grpc"))

		httpL := m.Match(cmux.HTTP1Fast())

		// NOTE: don't need to set tls config, because rootLis already use tls
		s.svr = grpc.NewServer()
		pb.RegisterWorkerServer(s.svr, s)

		grpcExitCh := make(chan struct{}, 1)
		s.wg.Add(1)
		go func() {
			err2 := s.svr.Serve(grpcL)
			if err2 != nil && !common.IsErrNetClosing(err2) && err2 != cmux.ErrListenerClosed {
				log.L().Error("gRPC server returned", log.ShortError(err2))
			}
			grpcExitCh <- struct{}{}
		}()
		go func(ctx context.Context) {
			defer s.wg.Done()
			select {
			case <-ctx.Done():
				if s.svr != nil {
					// GracefulStop can not cancel active stream RPCs
					// and the stream RPC may block on Recv or Send
					// so we use Stop instead to cancel all active RPCs
					s.svr.Stop()
				}
			case <-grpcExitCh:
			}
		}(s.ctx)

		s.httpWg.Add(1)
		go func() {
			s.httpWg.Done()
			InitStatus(httpL) // serve status
		}()

		s.closed.Store(false) // the server started now.
		return nil
	}()

	if startErr != nil {
		return startErr
	}

	log.L().Info("listening gRPC API and status request", zap.String("address", s.cfg.WorkerAddr))

	err := m.Serve()
	if err != nil && common.IsErrNetClosing(err) {
		err = nil
	}
	return terror.ErrWorkerStartService.Delegate(err)
}

// worker keepalive with master
// If worker loses connect from master, it would stop all task and try to connect master again.
func (s *Server) startKeepAlive() {
	s.kaWg.Add(1)
	s.kaCtx, s.kaCancel = context.WithCancel(s.ctx)
	go s.doStartKeepAlive()
}

func (s *Server) doStartKeepAlive() {
	defer s.kaWg.Done()
	s.KeepAlive()
}

func (s *Server) stopKeepAlive() {
	if s.kaCancel != nil {
		s.kaCancel()
		s.kaWg.Wait()
	}
}

func (s *Server) restartKeepAlive() {
	s.stopKeepAlive()
	s.startKeepAlive()
}

func (s *Server) observeRelayConfig(ctx context.Context, rev int64) error {
	var wg sync.WaitGroup
	for {
		relayCh := make(chan ha.RelaySource, 10)
		relayErrCh := make(chan error, 10)
		wg.Add(1)
		// use ctx1, cancel1 to make sure old watcher has been released
		ctx1, cancel1 := context.WithCancel(ctx)
		go func() {
			defer func() {
				close(relayCh)
				close(relayErrCh)
				wg.Done()
			}()
			ha.WatchRelayConfig(ctx1, s.etcdClient, s.cfg.Name, rev+1, relayCh, relayErrCh)
		}()
		err := s.handleRelayConfig(ctx1, relayCh, relayErrCh)
		cancel1()
		wg.Wait()

		if etcdutil.IsRetryableError(err) {
			rev = 0
			retryNum := 1
			for rev == 0 {
				select {
				case <-ctx.Done():
					return nil
				case <-time.After(500 * time.Millisecond):
					relaySource, rev1, err1 := ha.GetRelayConfig(s.etcdClient, s.cfg.Name)
					if err1 != nil {
						log.L().Error("get relay config from etcd failed, will retry later", zap.Error(err1), zap.Int("retryNum", retryNum))
						retryNum++
						if retryNum > retryGetRelayConfig && etcdutil.IsLimitedRetryableError(err1) {
							return err1
						}
						break
					}
					rev = rev1
					if relaySource == nil {
						if w := s.getSourceWorker(true); w != nil && w.startedRelayBySourceCfg {
							break
						}
						log.L().Info("didn't found relay config after etcd retryable error. Will stop relay now")
						err = s.disableRelay("")
						if err != nil {
							log.L().Error("fail to disableRelay after etcd retryable error", zap.Error(err))
							return err // return if failed to stop the worker.
						}
					} else {
						err2 := func() error {
							s.mu.Lock()
							defer s.mu.Unlock()

							if w := s.getSourceWorker(false); w != nil && w.cfg.SourceID == relaySource.SourceID {
								// we may face both relay config and subtask bound changed in a compaction error, so here
								// we check if observeSourceBound has started a worker
								// TODO: add a test for this situation
								if !w.relayEnabled.Load() {
									if err2 := w.EnableRelay(false); err2 != nil {
										return err2
									}
								}
								return nil
							}
							err = s.stopSourceWorker("", false, true)
							if err != nil {
								log.L().Error("fail to stop worker", zap.Error(err))
								return err // return if failed to stop the worker.
							}
							log.L().Info("will recover observeRelayConfig",
								zap.String("relay source", relaySource.SourceID))
							return s.enableRelay(relaySource, false)
						}()
						if err2 != nil {
							return err2
						}
					}
				}
			}
		} else {
			if err != nil {
				log.L().Error("observeRelayConfig is failed and will quit now", zap.Error(err))
			} else {
				log.L().Info("observeRelayConfig will quit now")
			}
			return err
		}
	}
}

// observeSourceBound will
// 1. keep bound relation updated from DM-master
// 2. keep enable-relay in source config updated. (TODO) This relies on DM-master re-put SourceBound after change it.
func (s *Server) observeSourceBound(ctx context.Context, rev int64) error {
	var wg sync.WaitGroup
	for {
		sourceBoundCh := make(chan ha.SourceBound, 10)
		sourceBoundErrCh := make(chan error, 10)
		wg.Add(1)
		// use ctx1, cancel1 to make sure old watcher has been released
		ctx1, cancel1 := context.WithCancel(ctx)
		go func() {
			defer func() {
				close(sourceBoundCh)
				close(sourceBoundErrCh)
				wg.Done()
			}()
			ha.WatchSourceBound(ctx1, s.etcdClient, s.cfg.Name, rev+1, sourceBoundCh, sourceBoundErrCh)
		}()
		err := s.handleSourceBound(ctx1, sourceBoundCh, sourceBoundErrCh)
		cancel1()
		wg.Wait()

		if etcdutil.IsRetryableError(err) {
			rev = 0
			retryNum := 1
			for rev == 0 {
				select {
				case <-ctx.Done():
					return nil
				case <-time.After(500 * time.Millisecond):
					bound, cfg, rev1, err1 := ha.GetSourceBoundConfig(s.etcdClient, s.cfg.Name)
					if err1 != nil {
						log.L().Error("get source bound from etcd failed, will retry later", zap.Error(err1), zap.Int("retryNum", retryNum))
						retryNum++
						if retryNum > retryGetSourceBoundConfig && etcdutil.IsLimitedRetryableError(err1) {
							return err1
						}
						break
					}
					rev = rev1
					if bound.IsEmpty() {
						err = s.disableHandleSubtasks("")
						if err != nil {
							log.L().Error("fail to disableHandleSubtasks after etcd retryable error", zap.Error(err))
							return err // return if failed to stop the worker.
						}
					} else {
						err2 := func() error {
							s.mu.Lock()
							defer s.mu.Unlock()

							if w := s.getSourceWorker(false); w != nil && w.cfg.SourceID == bound.Source {
								// we may face both relay config and subtask bound changed in a compaction error, so here
								// we check if observeRelayConfig has started a worker
								// TODO: add a test for this situation
								if !w.subTaskEnabled.Load() {
									if err2 := w.EnableHandleSubtasks(); err2 != nil {
										return err2
									}
								}
								return nil
							}
							err = s.stopSourceWorker("", false, true)
							if err != nil {
								log.L().Error("fail to stop worker", zap.Error(err))
								return err // return if failed to stop the worker.
							}
							log.L().Info("will recover observeSourceBound",
								zap.String("relay source", cfg.SourceID))
							return s.enableHandleSubtasks(cfg, false)
						}()
						if err2 != nil {
							if terror.ErrWorkerServerClosed.Equal(err2) {
								// return nil to exit the loop in caller
								return nil
							}
							return err2
						}
					}
				}
			}
		} else {
			if err != nil {
				log.L().Error("observeSourceBound is failed and will quit now", zap.Error(err))
			} else {
				log.L().Info("observeSourceBound will quit now")
			}
			return err
		}
	}
}

func (s *Server) doClose() {
	if s.closed.Load() {
		return
	}
	// stop server in advance, stop receiving source bound and relay bound
	s.runCancel()
	s.runWg.Wait()

	// stop worker and wait for return(we already lock the whole Sever, so no need use lock to get source worker)
	if w := s.getSourceWorker(true); w != nil {
		w.Stop(true)
	}

	// close listener at last, so we can get status from it if worker failed to close in previous step
	if s.rootLis != nil {
		err2 := s.rootLis.Close()
		if err2 != nil && !common.IsErrNetClosing(err2) {
			log.L().Error("fail to close net listener", log.ShortError(err2))
		}
	}
	s.httpWg.Wait()

	s.closed.Store(true)
}

// Close closes the RPC server, this function can be called multiple times.
func (s *Server) Close() {
	s.closeMu.Lock()
	defer s.closeMu.Unlock()
	s.doClose() // we should stop current sync first, otherwise master may schedule task on new worker while we are closing
	s.stopKeepAlive()

	s.cancel()
	s.wg.Wait()

	if s.etcdClient != nil {
		s.etcdClient.Close()
	}
	s.calledClose = true
}

// if needLock is false, we should make sure Server has been locked in caller.
func (s *Server) getSourceWorker(needLock bool) *SourceWorker {
	if needLock {
		s.mu.Lock()
		defer s.mu.Unlock()
	}
	return s.worker
}

// if needLock is false, we should make sure Server has been locked in caller.
func (s *Server) setWorker(worker *SourceWorker, needLock bool) {
	if needLock {
		s.mu.Lock()
		defer s.mu.Unlock()
	}
	s.worker = worker
}

// nolint:unparam
func (s *Server) getSourceStatus(needLock bool) pb.SourceStatus {
	if needLock {
		s.mu.Lock()
		defer s.mu.Unlock()
	}
	return s.sourceStatus
}

// TODO: move some call to setWorker/getOrStartWorker.
func (s *Server) setSourceStatus(source string, err error, needLock bool) {
	if needLock {
		s.mu.Lock()
		defer s.mu.Unlock()
	}
	// now setSourceStatus will be concurrently called. skip setting a source status if worker has been closed
	if s.getSourceWorker(false) == nil && source != "" {
		return
	}
	s.sourceStatus = pb.SourceStatus{
		Source: source,
		Worker: s.cfg.Name,
	}
	if err != nil {
		s.sourceStatus.Result = &pb.ProcessResult{
			Errors: []*pb.ProcessError{
				unit.NewProcessError(err),
			},
		}
	}
}

// if sourceID is set to "", worker will be closed directly
// if sourceID is not "", we will check sourceID with w.cfg.SourceID.
func (s *Server) stopSourceWorker(sourceID string, needLock, graceful bool) error {
	if needLock {
		s.mu.Lock()
		defer s.mu.Unlock()
	}
	w := s.getSourceWorker(false)
	if w == nil {
		log.L().Warn("worker has not been started, no need to stop", zap.String("source", sourceID))
		return nil // no need to stop because not started yet
	}
	if sourceID != "" && w.cfg.SourceID != sourceID {
		return terror.ErrWorkerSourceNotMatch
	}
	s.UpdateKeepAliveTTL(s.cfg.KeepAliveTTL)
	s.setWorker(nil, false)
	s.setSourceStatus("", nil, false)
	w.Stop(graceful)
	return nil
}

func (s *Server) handleSourceBound(ctx context.Context, boundCh chan ha.SourceBound, errCh chan error) error {
OUTER:
	for {
		select {
		case <-ctx.Done():
			break OUTER
		case bound, ok := <-boundCh:
			if !ok {
				break OUTER
			}
			log.L().Info("receive source bound", zap.Stringer("bound", bound), zap.Bool("is deleted", bound.IsDeleted))
			err := s.operateSourceBound(bound)
			s.setSourceStatus(bound.Source, err, true)
			if err != nil {
				opErrCounter.WithLabelValues(s.cfg.Name, opErrTypeSourceBound).Inc()
				log.L().Error("fail to operate sourceBound on worker", zap.Stringer("bound", bound), zap.Bool("is deleted", bound.IsDeleted), zap.Error(err))
				if etcdutil.IsRetryableError(err) {
					return err
				}
			}
		case err, ok := <-errCh:
			if !ok {
				break OUTER
			}
			// TODO: Deal with err
			log.L().Error("WatchSourceBound received an error", zap.Error(err))
			if etcdutil.IsRetryableError(err) {
				return err
			}
		}
	}
	log.L().Info("handleSourceBound will quit now")
	return nil
}

func (s *Server) handleRelayConfig(ctx context.Context, relayCh chan ha.RelaySource, errCh chan error) error {
OUTER:
	for {
		select {
		case <-ctx.Done():
			break OUTER
		case relaySource, ok := <-relayCh:
			if !ok {
				break OUTER
			}
			log.L().Info("receive relay source", zap.String("relay source", relaySource.Source), zap.Bool("is deleted", relaySource.IsDeleted))
			err := s.operateRelaySource(relaySource)
			s.setSourceStatus(relaySource.Source, err, true)
			if err != nil {
				opErrCounter.WithLabelValues(s.cfg.Name, opErrTypeRelaySource).Inc()
				log.L().Error("fail to operate relay source on worker",
					zap.String("relay source", relaySource.Source),
					zap.Bool("is deleted", relaySource.IsDeleted),
					zap.Error(err))
				if etcdutil.IsRetryableError(err) {
					return err
				}
			}
		case err, ok := <-errCh:
			// currently no value is sent to errCh
			if !ok {
				break OUTER
			}
			// TODO: Deal with err
			log.L().Error("WatchRelayConfig received an error", zap.Error(err))
			if etcdutil.IsRetryableError(err) {
				return err
			}
		}
	}
	log.L().Info("worker server is closed, handleRelayConfig will quit now")
	return nil
}

func (s *Server) operateSourceBound(bound ha.SourceBound) error {
	if bound.IsDeleted {
		return s.disableHandleSubtasks(bound.Source)
	}
	scm, _, err := ha.GetSourceCfg(s.etcdClient, bound.Source, bound.Revision)
	if err != nil {
		// TODO: need retry
		return err
	}
	sourceCfg, ok := scm[bound.Source]
	if !ok {
		return terror.ErrWorkerFailToGetSourceConfigFromEtcd.Generate(bound.Source)
	}
	return s.enableHandleSubtasks(sourceCfg, true)
}

func (s *Server) enableHandleSubtasks(sourceCfg *config.SourceConfig, needLock bool) error {
	if needLock {
		s.mu.Lock()
		defer s.mu.Unlock()
	}

	w, err := s.getOrStartWorker(sourceCfg, false)
	s.setSourceStatus(sourceCfg.SourceID, err, false)
	if err != nil {
		return err
	}

	if sourceCfg.EnableRelay {
		log.L().Info("will start relay by `enable-relay` in source config")
		if err2 := w.EnableRelay(true); err2 != nil {
			log.L().Error("found a `enable-relay: true` source, but failed to enable relay for DM worker",
				zap.Error(err2))
			return err2
		}
	} else if w.startedRelayBySourceCfg {
		log.L().Info("will disable relay by `enable-relay: false` in source config")
		w.DisableRelay()
	}

	if err2 := w.EnableHandleSubtasks(); err2 != nil {
		s.setSourceStatus(sourceCfg.SourceID, err2, false)
		return err2
	}
	return nil
}

func (s *Server) disableHandleSubtasks(source string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	w := s.getSourceWorker(false)
	if w == nil {
		log.L().Warn("worker has already stopped before DisableHandleSubtasks", zap.String("source", source))
		return nil
	}

	w.DisableHandleSubtasks()

	// now the worker is unbound, stop relay if it's started by source config
	if w.cfg.EnableRelay && w.startedRelayBySourceCfg {
		log.L().Info("stop relay because the source is unbound")
		w.DisableRelay()
	}

	var err error
	if !w.relayEnabled.Load() {
		log.L().Info("relay is not enabled after disabling subtask, so stop worker")
		err = s.stopSourceWorker(source, false, true)
	}
	return err
}

func (s *Server) operateRelaySource(relaySource ha.RelaySource) error {
	if relaySource.IsDeleted {
		return s.disableRelay(relaySource.Source)
	}
	scm, _, err := ha.GetSourceCfg(s.etcdClient, relaySource.Source, relaySource.Revision)
	if err != nil {
		// TODO: need retry
		return err
	}
	sourceCfg, ok := scm[relaySource.Source]
	if !ok {
		return terror.ErrWorkerFailToGetSourceConfigFromEtcd.Generate(relaySource.Source)
	}
	return s.enableRelay(sourceCfg, true)
}

func (s *Server) enableRelay(sourceCfg *config.SourceConfig, needLock bool) error {
	if needLock {
		s.mu.Lock()
		defer s.mu.Unlock()
	}

	w, err2 := s.getOrStartWorker(sourceCfg, false)
	s.setSourceStatus(sourceCfg.SourceID, err2, false)
	if err2 != nil {
		// if DM-worker can't handle pre-assigned source before keepalive, it simply exits with the error,
		// because no re-assigned mechanism exists for keepalived DM-worker yet.
		return err2
	}
	if err2 = w.EnableRelay(false); err2 != nil {
		s.setSourceStatus(sourceCfg.SourceID, err2, false)
		return err2
	}
	s.UpdateKeepAliveTTL(s.cfg.RelayKeepAliveTTL)
	return nil
}

func (s *Server) disableRelay(source string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	w := s.getSourceWorker(false)
	if w == nil {
		log.L().Warn("worker has already stopped before DisableRelay", zap.Any("relaySource", source))
		return nil
	}
	s.UpdateKeepAliveTTL(s.cfg.KeepAliveTTL)
	w.DisableRelay()
	var err error
	if !w.subTaskEnabled.Load() {
		log.L().Info("subtask is not enabled after disabling relay, so stop worker")
		err = s.stopSourceWorker(source, false, true)
	}
	return err
}

// QueryStatus implements WorkerServer.QueryStatus.
func (s *Server) QueryStatus(ctx context.Context, req *pb.QueryStatusRequest) (*pb.QueryStatusResponse, error) {
	log.L().Info("", zap.String("request", "QueryStatus"), zap.Stringer("payload", req))

	sourceStatus := s.getSourceStatus(true)
	sourceStatus.Worker = s.cfg.Name
	resp := &pb.QueryStatusResponse{
		Result:       true,
		SourceStatus: &sourceStatus,
	}

	w := s.getSourceWorker(true)
	if w == nil {
		log.L().Warn("fail to call QueryStatus, because no mysql source is being handled in the worker")
		resp.Result = false
		resp.Msg = terror.ErrWorkerNoStart.Error()
		return resp, nil
	}

	var err error
	resp.SubTaskStatus, sourceStatus.RelayStatus, err = w.QueryStatus(ctx, req.Name)

	if err != nil {
		resp.Msg = fmt.Sprintf("error when get master status: %v", err)
	} else if len(resp.SubTaskStatus) == 0 {
		resp.Msg = "no sub task started"
	}
	return resp, nil
}

// PurgeRelay implements WorkerServer.PurgeRelay.
func (s *Server) PurgeRelay(ctx context.Context, req *pb.PurgeRelayRequest) (*pb.CommonWorkerResponse, error) {
	log.L().Info("", zap.String("request", "PurgeRelay"), zap.Stringer("payload", req))
	w := s.getSourceWorker(true)
	if w == nil {
		log.L().Warn("fail to call StartSubTask, because no mysql source is being handled in the worker")
		return makeCommonWorkerResponse(terror.ErrWorkerNoStart.Generate()), nil
	}

	err := w.PurgeRelay(ctx, req)
	if err != nil {
		log.L().Error("fail to purge relay", zap.String("request", "PurgeRelay"), zap.Stringer("payload", req), zap.Error(err))
	}
	return makeCommonWorkerResponse(err), nil
}

// OperateSchema operates schema for an upstream table.
func (s *Server) OperateSchema(ctx context.Context, req *pb.OperateWorkerSchemaRequest) (*pb.CommonWorkerResponse, error) {
	log.L().Info("", zap.String("request", "OperateSchema"), zap.Stringer("payload", req))

	w := s.getSourceWorker(true)
	if w == nil {
		log.L().Warn("fail to call OperateSchema, because no mysql source is being handled in the worker")
		return makeCommonWorkerResponse(terror.ErrWorkerNoStart.Generate()), nil
	}
	w.RLock()
	sourceID := w.cfg.SourceID
	w.RUnlock()
	if req.Source != sourceID {
		log.L().Error("fail to call OperateSchema, because source mismatch", zap.String("request", req.Source), zap.String("current", sourceID))
		return makeCommonWorkerResponse(terror.ErrWorkerSourceNotMatch.Generate()), nil
	}

	schema, err := w.OperateSchema(ctx, req)
	if err != nil {
		return makeCommonWorkerResponse(err), nil
	}
	return &pb.CommonWorkerResponse{
		Result: true,
		Msg:    schema, // if any schema return for `GET`, we place it in the `msg` field now.
		Source: req.Source,
		Worker: s.cfg.Name,
	}, nil
}

func (s *Server) getOrStartWorker(cfg *config.SourceConfig, needLock bool) (*SourceWorker, error) {
	if needLock {
		s.mu.Lock()
		defer s.mu.Unlock()
	}

	if w := s.getSourceWorker(false); w != nil {
		if w.cfg.SourceID == cfg.SourceID {
			log.L().Info("mysql source is being handled", zap.String("sourceID", s.worker.cfg.SourceID))
			return w, nil
		}
		return nil, terror.ErrWorkerAlreadyStart.Generate(w.name, w.cfg.SourceID, cfg.SourceID)
	}

	log.L().Info("will start a new worker", zap.String("sourceID", cfg.SourceID))
	w, err := NewSourceWorker(cfg, s.etcdClient, s.cfg.Name, s.cfg.RelayDir)
	if err != nil {
		return nil, err
	}
	s.setWorker(w, false)

	go w.Start()

	isStarted := utils.WaitSomething(50, 100*time.Millisecond, func() bool {
		return !w.closed.Load()
	})
	if !isStarted {
		// TODO: add more mechanism to wait or un-bound the source
		return nil, terror.ErrWorkerNoStart
	}
	return w, nil
}

func makeCommonWorkerResponse(reqErr error) *pb.CommonWorkerResponse {
	resp := &pb.CommonWorkerResponse{
		Result: true,
	}
	if reqErr != nil {
		resp.Result = false
		resp.Msg = reqErr.Error()
	}
	return resp
}

// all subTask in subTaskCfgs should have same source
// this function return the min location in all subtasks, used for relay's location.
func getMinLocInAllSubTasks(ctx context.Context, subTaskCfgs map[string]config.SubTaskConfig) (minLoc *binlog.Location, err error) {
	for _, subTaskCfg := range subTaskCfgs {
		loc, err := getMinLocForSubTaskFunc(ctx, subTaskCfg)
		if err != nil {
			return nil, err
		}

		if loc == nil {
			continue
		}

		if minLoc == nil {
			minLoc = loc
		} else if binlog.CompareLocation(*minLoc, *loc, subTaskCfg.EnableGTID) >= 1 {
			minLoc = loc
		}
	}

	return minLoc, nil
}

func getMinLocForSubTask(ctx context.Context, subTaskCfg config.SubTaskConfig) (minLoc *binlog.Location, err error) {
	if subTaskCfg.Mode == config.ModeFull {
		return nil, nil
	}
	subTaskCfg2, err := subTaskCfg.DecryptPassword()
	if err != nil {
		return nil, errors.Annotate(err, "get min position from checkpoint")
	}

	tctx := tcontext.NewContext(ctx, log.L())
	checkpoint := syncer.NewRemoteCheckPoint(tctx, subTaskCfg2, nil, subTaskCfg2.SourceID)
	err = checkpoint.Init(tctx)
	if err != nil {
		return nil, errors.Annotate(err, "get min position from checkpoint")
	}
	defer checkpoint.Close()

	err = checkpoint.Load(tctx)
	if err != nil {
		return nil, errors.Annotate(err, "get min position from checkpoint")
	}

	location := checkpoint.GlobalPoint()
	return &location, nil
}

// HandleError handle error.
func (s *Server) HandleError(ctx context.Context, req *pb.HandleWorkerErrorRequest) (*pb.CommonWorkerResponse, error) {
	log.L().Info("", zap.String("request", "HandleError"), zap.Stringer("payload", req))

	w := s.getSourceWorker(true)
	if w == nil {
		log.L().Warn("fail to call HandleError, because no mysql source is being handled in the worker")
		return makeCommonWorkerResponse(terror.ErrWorkerNoStart.Generate()), nil
	}

	msg, err := w.HandleError(ctx, req)
	if err != nil {
		return makeCommonWorkerResponse(err), nil
	}
	return &pb.CommonWorkerResponse{
		Result: true,
		Worker: s.cfg.Name,
		Msg:    msg,
	}, nil
}

// GetWorkerCfg get worker config.
func (s *Server) GetWorkerCfg(ctx context.Context, req *pb.GetWorkerCfgRequest) (*pb.GetWorkerCfgResponse, error) {
	log.L().Info("", zap.String("request", "GetWorkerCfg"), zap.Stringer("payload", req))
	var err error
	resp := &pb.GetWorkerCfgResponse{}

	resp.Cfg, err = s.cfg.Toml()
	return resp, err
}

// CheckSubtasksCanUpdate check if input subtask cfg can be updated.
func (s *Server) CheckSubtasksCanUpdate(ctx context.Context, req *pb.CheckSubtasksCanUpdateRequest) (*pb.CheckSubtasksCanUpdateResponse, error) {
	log.L().Info("", zap.String("request", "CheckSubtasksCanUpdate"), zap.Stringer("payload", req))
	resp := &pb.CheckSubtasksCanUpdateResponse{}
	defer func() {
		log.L().Info("", zap.String("request", "CheckSubtasksCanUpdate"), zap.Stringer("resp", resp))
	}()
	w := s.getSourceWorker(true)
	if w == nil {
		msg := "fail to call CheckSubtasksCanUpdate, because no mysql source is being handled in the worker"
		log.L().Warn(msg)
		resp.Msg = msg
		return resp, nil
	}
	cfg := config.NewSubTaskConfig()
	if err := cfg.Decode(req.SubtaskCfgTomlString, false); err != nil {
		resp.Msg = err.Error()
		// nolint:nilerr
		return resp, nil
	}
	if err := w.CheckCfgCanUpdated(cfg); err != nil {
		resp.Msg = err.Error()
		// nolint:nilerr
		return resp, nil
	}
	resp.Success = true
	return resp, nil
}

func (s *Server) GetWorkerValidatorStatus(ctx context.Context, req *pb.GetValidationStatusRequest) (*pb.GetValidationStatusResponse, error) {
	log.L().Info("", zap.String("request", "GetWorkerValidateStatus"), zap.Stringer("payload", req))

	resp := &pb.GetValidationStatusResponse{
		Result: true,
	}
	w := s.getSourceWorker(true)
	if w == nil {
		log.L().Warn("fail to call GetWorkerValidateStatus, because no mysql source is being handled in the worker")
		resp.Result = false
		resp.Msg = terror.ErrWorkerNoStart.Error()
		return resp, nil
	}
	validatorStatus, err := w.GetValidatorStatus(req.TaskName)
	if err != nil {
		return resp, err
	}
	res, err := w.GetValidatorTableStatus(req.TaskName, req.FilterStatus)
	if err != nil {
		return resp, err
	}

	resp.Validators = []*pb.ValidationStatus{validatorStatus}
	resp.TableStatuses = res
	return resp, nil
}

func (s *Server) GetValidatorError(ctx context.Context, req *pb.GetValidationErrorRequest) (*pb.GetValidationErrorResponse, error) {
	w := s.getSourceWorker(true)
	resp := &pb.GetValidationErrorResponse{
		Result: true,
	}
	if w == nil {
		log.L().Warn("fail to get validator error, because no mysql source is being handled in the worker")
		resp.Result = false
		resp.Msg = terror.ErrWorkerNoStart.Error()
		return resp, nil
	}
	validatorErrs, err := w.GetWorkerValidatorErr(req.TaskName, req.ErrState)
	if err != nil {
		resp.Msg = err.Error()
		resp.Result = false
	} else {
		resp.Error = validatorErrs
	}
	return resp, nil
}

func (s *Server) OperateValidatorError(ctx context.Context, req *pb.OperateValidationErrorRequest) (*pb.OperateValidationErrorResponse, error) {
	log.L().Info("operate validation error", zap.Stringer("payload", req))
	w := s.getSourceWorker(true)
	resp := &pb.OperateValidationErrorResponse{
		Result: true,
	}
	if w == nil {
		log.L().Warn("fail to operate validator error, because no mysql source is being handled in the worker")
		resp.Result = false
		resp.Msg = terror.ErrWorkerNoStart.Error()
		return resp, nil
	}
	err := w.OperateWorkerValidatorErr(req.TaskName, req.Op, req.ErrId, req.IsAllError)
	if err != nil {
		resp.Result = false
		resp.Msg = err.Error()
		//nolint:nilerr
		return resp, nil
	}
	//nolint:nilerr
	return resp, nil
}
