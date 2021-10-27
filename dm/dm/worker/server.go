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

	"github.com/pingcap/dm/dm/common"
	"github.com/pingcap/dm/dm/config"
	"github.com/pingcap/dm/dm/pb"
	"github.com/pingcap/dm/dm/unit"
	"github.com/pingcap/dm/pkg/binlog"
	tcontext "github.com/pingcap/dm/pkg/context"
	"github.com/pingcap/dm/pkg/etcdutil"
	"github.com/pingcap/dm/pkg/ha"
	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/dm/pkg/terror"
	"github.com/pingcap/dm/pkg/utils"
	"github.com/pingcap/dm/syncer"

	"github.com/pingcap/errors"
	toolutils "github.com/pingcap/tidb-tools/pkg/utils"
	"github.com/soheilhy/cmux"
	"go.etcd.io/etcd/clientv3"
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
	sync.Mutex
	wg     sync.WaitGroup
	kaWg   sync.WaitGroup
	closed atomic.Bool
	ctx    context.Context
	cancel context.CancelFunc

	kaCtx    context.Context
	kaCancel context.CancelFunc

	cfg *Config

	rootLis    net.Listener
	svr        *grpc.Server
	worker     *SourceWorker
	etcdClient *clientv3.Client

	// relay status will never be put in server.sourceStatus
	sourceStatus pb.SourceStatus
}

// NewServer creates a new Server.
func NewServer(cfg *Config) *Server {
	s := Server{
		cfg: cfg,
	}
	s.closed.Store(true) // not start yet
	return &s
}

// Start starts to serving.
func (s *Server) Start() error {
	log.L().Info("starting dm-worker server")
	RegistryMetrics()
	s.ctx, s.cancel = context.WithCancel(context.Background())
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
	})
	if err != nil {
		return err
	}

	s.setWorker(nil, true)

	s.wg.Add(1)
	go func() {
		s.runBackgroundJob(s.ctx)
		s.wg.Done()
	}()

	s.wg.Add(1)
	go func() {
		s.syncMasterEndpoints(s.ctx)
		s.wg.Done()
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

	s.wg.Add(1)
	go func(ctx context.Context) {
		defer s.wg.Done()
		// TODO: handle fatal error from observeRelayConfig
		//nolint:errcheck
		s.observeRelayConfig(ctx, revRelay)
	}(s.ctx)

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

	s.wg.Add(1)
	go func(ctx context.Context) {
		defer s.wg.Done()
		for {
			err1 := s.observeSourceBound(ctx, revBound)
			if err1 == nil {
				return
			}
			s.restartKeepAlive()
		}
	}(s.ctx)

	// create a cmux
	m := cmux.New(s.rootLis)

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

	httpExitCh := make(chan struct{}, 1)
	s.wg.Add(1)
	go func() {
		InitStatus(httpL) // serve status
		httpExitCh <- struct{}{}
	}()
	go func(ctx context.Context) {
		defer s.wg.Done()
		select {
		case <-ctx.Done():
			if s.rootLis != nil {
				err2 := s.rootLis.Close()
				if err2 != nil && !common.IsErrNetClosing(err2) {
					log.L().Error("fail to close net listener", log.ShortError(err2))
				}
			}
		case <-httpExitCh:
		}
	}(s.ctx)

	s.closed.Store(false)
	log.L().Info("listening gRPC API and status request", zap.String("address", s.cfg.WorkerAddr))
	err = m.Serve()
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

func (s *Server) syncMasterEndpoints(ctx context.Context) {
	lastClientUrls := []string{}
	clientURLs := []string{}

	updateF := func() {
		clientURLs = clientURLs[:0]
		resp, err := s.etcdClient.MemberList(ctx)
		if err != nil {
			log.L().Error("can't get etcd member list", zap.Error(err))
			return
		}

		for _, m := range resp.Members {
			clientURLs = append(clientURLs, m.GetClientURLs()...)
		}
		if utils.NonRepeatStringsEqual(clientURLs, lastClientUrls) {
			log.L().Debug("etcd member list doesn't change", zap.Strings("client URLs", clientURLs))
			return
		}
		log.L().Info("will sync endpoints to", zap.Strings("client URLs", clientURLs))
		s.etcdClient.SetEndpoints(clientURLs...)
		lastClientUrls = make([]string, len(clientURLs))
		copy(lastClientUrls, clientURLs)
	}

	for {
		updateF()

		select {
		case <-ctx.Done():
			return
		case <-time.After(syncMasterEndpointsTime):
		}
	}
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
						err = s.disableRelay("")
						if err != nil {
							log.L().Error("fail to disableRelay after etcd retryable error", zap.Error(err))
							return err // return if failed to stop the worker.
						}
					} else {
						err2 := func() error {
							s.Lock()
							defer s.Unlock()

							if w := s.getWorker(false); w != nil && w.cfg.SourceID == relaySource.SourceID {
								// we may face both relay config and subtask bound changed in a compaction error, so here
								// we check if observeSourceBound has started a worker
								// TODO: add a test for this situation
								if !w.relayEnabled.Load() {
									if err2 := w.EnableRelay(); err2 != nil {
										return err2
									}
								}
								return nil
							}
							err = s.stopWorker("", false)
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
							s.Lock()
							defer s.Unlock()

							if w := s.getWorker(false); w != nil && w.cfg.SourceID == bound.Source {
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
							err = s.stopWorker("", false)
							if err != nil {
								log.L().Error("fail to stop worker", zap.Error(err))
								return err // return if failed to stop the worker.
							}
							log.L().Info("will recover observeSourceBound",
								zap.String("relay source", cfg.SourceID))
							return s.enableHandleSubtasks(cfg, false)
						}()
						if err2 != nil {
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
	s.cancel()
	// close server in advance, stop receiving source bound and relay bound
	s.wg.Wait()

	s.Lock()
	defer s.Unlock()
	if s.closed.Load() {
		return
	}
	// close worker and wait for return
	if w := s.getWorker(false); w != nil {
		w.Close()
	}
	s.closed.Store(true)
}

// Close close the RPC server, this function can be called multiple times.
func (s *Server) Close() {
	s.stopKeepAlive()
	s.doClose()
}

// if needLock is false, we should make sure Server has been locked in caller.
func (s *Server) getWorker(needLock bool) *SourceWorker {
	if needLock {
		s.Lock()
		defer s.Unlock()
	}
	return s.worker
}

// if needLock is false, we should make sure Server has been locked in caller.
func (s *Server) setWorker(worker *SourceWorker, needLock bool) {
	if needLock {
		s.Lock()
		defer s.Unlock()
	}
	s.worker = worker
}

// nolint:unparam
func (s *Server) getSourceStatus(needLock bool) pb.SourceStatus {
	if needLock {
		s.Lock()
		defer s.Unlock()
	}
	return s.sourceStatus
}

// TODO: move some call to setWorker/getOrStartWorker.
func (s *Server) setSourceStatus(source string, err error, needLock bool) {
	if needLock {
		s.Lock()
		defer s.Unlock()
	}
	// now setSourceStatus will be concurrently called. skip setting a source status if worker has been closed
	if s.getWorker(false) == nil && source != "" {
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
func (s *Server) stopWorker(sourceID string, needLock bool) error {
	if needLock {
		s.Lock()
		defer s.Unlock()
	}
	w := s.getWorker(false)
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
	w.Close()
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
		s.Lock()
		defer s.Unlock()
	}

	w, err := s.getOrStartWorker(sourceCfg, false)
	s.setSourceStatus(sourceCfg.SourceID, err, false)
	if err != nil {
		return err
	}
	if err2 := w.EnableHandleSubtasks(); err2 != nil {
		s.setSourceStatus(sourceCfg.SourceID, err2, false)
		return err2
	}
	return nil
}

func (s *Server) disableHandleSubtasks(source string) error {
	s.Lock()
	defer s.Unlock()
	w := s.getWorker(false)
	if w == nil {
		log.L().Warn("worker has already stopped before DisableHandleSubtasks", zap.String("source", source))
		return nil
	}
	w.DisableHandleSubtasks()
	var err error
	if !w.relayEnabled.Load() {
		log.L().Info("relay is not enabled after disabling subtask, so stop worker")
		err = s.stopWorker(source, false)
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
		s.Lock()
		defer s.Unlock()
	}

	w, err2 := s.getOrStartWorker(sourceCfg, false)
	s.setSourceStatus(sourceCfg.SourceID, err2, false)
	if err2 != nil {
		// if DM-worker can't handle pre-assigned source before keepalive, it simply exits with the error,
		// because no re-assigned mechanism exists for keepalived DM-worker yet.
		return err2
	}
	if err2 = w.EnableRelay(); err2 != nil {
		s.setSourceStatus(sourceCfg.SourceID, err2, false)
		return err2
	}
	s.UpdateKeepAliveTTL(s.cfg.RelayKeepAliveTTL)
	return nil
}

func (s *Server) disableRelay(source string) error {
	s.Lock()
	defer s.Unlock()
	w := s.getWorker(false)
	if w == nil {
		log.L().Warn("worker has already stopped before DisableRelay", zap.Any("relaySource", source))
		return nil
	}
	s.UpdateKeepAliveTTL(s.cfg.KeepAliveTTL)
	w.DisableRelay()
	var err error
	if !w.subTaskEnabled.Load() {
		log.L().Info("subtask is not enabled after disabling relay, so stop worker")
		err = s.stopWorker(source, false)
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

	w := s.getWorker(true)
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
	w := s.getWorker(true)
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

	w := s.getWorker(true)
	if w == nil {
		log.L().Warn("fail to call OperateSchema, because no mysql source is being handled in the worker")
		return makeCommonWorkerResponse(terror.ErrWorkerNoStart.Generate()), nil
	} else if req.Source != w.cfg.SourceID {
		log.L().Error("fail to call OperateSchema, because source mismatch", zap.String("request", req.Source), zap.String("current", w.cfg.SourceID))
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
		s.Lock()
		defer s.Unlock()
	}

	if w := s.getWorker(false); w != nil {
		if w.cfg.SourceID == cfg.SourceID {
			log.L().Info("mysql source is being handled", zap.String("sourceID", s.worker.cfg.SourceID))
			return w, nil
		}
		return nil, terror.ErrWorkerAlreadyStart.Generate(w.name, w.cfg.SourceID, cfg.SourceID)
	}

	log.L().Info("will start a new worker", zap.String("sourceID", cfg.SourceID))
	w, err := NewSourceWorker(cfg, s.etcdClient, s.cfg.Name)
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
	checkpoint := syncer.NewRemoteCheckPoint(tctx, subTaskCfg2, subTaskCfg2.SourceID)
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

	w := s.getWorker(true)
	if w == nil {
		log.L().Warn("fail to call HandleError, because no mysql source is being handled in the worker")
		return makeCommonWorkerResponse(terror.ErrWorkerNoStart.Generate()), nil
	}

	if err := w.HandleError(ctx, req); err != nil {
		return makeCommonWorkerResponse(err), nil
	}
	return &pb.CommonWorkerResponse{
		Result: true,
		Worker: s.cfg.Name,
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
