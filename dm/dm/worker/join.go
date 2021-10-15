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
	"strings"
	"time"

	"github.com/pingcap/failpoint"
	toolutils "github.com/pingcap/tidb-tools/pkg/utils"
	"go.uber.org/zap"
	"google.golang.org/grpc"

	"github.com/pingcap/dm/dm/pb"
	"github.com/pingcap/dm/pkg/ha"
	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/dm/pkg/terror"
	"github.com/pingcap/dm/pkg/utils"
)

// GetJoinURLs gets the endpoints from the join address.
func GetJoinURLs(addrs string) []string {
	// TODO: handle pm1=xxxx:1234,pm2=xxxx:1234,pm3=xxxx:1234
	return strings.Split(addrs, ",")
}

// JoinMaster let dm-worker join the cluster with the specified master endpoints.
func (s *Server) JoinMaster(endpoints []string) error {
	// TODO: grpc proxy
	tls, err := toolutils.NewTLS(s.cfg.SSLCA, s.cfg.SSLCert, s.cfg.SSLKey, s.cfg.AdvertiseAddr, s.cfg.CertAllowedCN)
	if err != nil {
		return terror.ErrWorkerTLSConfigNotValid.Delegate(err)
	}

	// join doesn't support to be canceled now, because it can return at most in (3s+3s) * len(endpoints).
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	req := &pb.RegisterWorkerRequest{
		Name:    s.cfg.Name,
		Address: s.cfg.AdvertiseAddr,
	}

	var errorStr string
	for _, endpoint := range endpoints {
		ctx1, cancel1 := context.WithTimeout(ctx, 3*time.Second)
		//nolint:staticcheck
		conn, err := grpc.DialContext(ctx1, utils.UnwrapScheme(endpoint), grpc.WithBlock(), tls.ToGRPCDialOption(), grpc.WithBackoffMaxDelay(3*time.Second))
		cancel1()
		if err != nil {
			if conn != nil {
				conn.Close()
			}
			log.L().Error("fail to dial dm-master", zap.String("endpoint", endpoint), zap.Error(err))
			errorStr = err.Error()
			continue
		}
		client := pb.NewMasterClient(conn)
		ctx1, cancel1 = context.WithTimeout(ctx, 3*time.Second)
		resp, err := client.RegisterWorker(ctx1, req)
		cancel1()
		conn.Close()
		if err != nil {
			log.L().Error("fail to register worker", zap.String("endpoint", endpoint), zap.Error(err))
			errorStr = err.Error()
			continue
		}
		if !resp.GetResult() {
			log.L().Error("fail to register worker", zap.String("endpoint", endpoint), zap.String("error", resp.Msg))
			errorStr = resp.Msg
			continue
		}
		return nil
	}
	return terror.ErrWorkerFailConnectMaster.Generate(endpoints, errorStr)
}

// KeepAlive attempts to keep the lease of the server alive forever.
func (s *Server) KeepAlive() {
	for {
		log.L().Info("start to keepalive with master")

		failpoint.Inject("FailToKeepAlive", func(val failpoint.Value) {
			workerStrings := val.(string)
			if strings.Contains(workerStrings, s.cfg.Name) {
				log.L().Info("worker keep alive failed", zap.String("failpoint", "FailToKeepAlive"))
				failpoint.Goto("bypass")
			}
		})

		{
			err1 := ha.KeepAlive(s.kaCtx, s.etcdClient, s.cfg.Name, s.cfg.KeepAliveTTL)
			log.L().Warn("keepalive with master goroutine paused", zap.Error(err1))
		}

		failpoint.Label("bypass")

		// TODO: report the error.
		err := s.stopWorker("", true)
		if err != nil {
			log.L().Error("fail to stop worker", zap.Error(err))
			return // return if failed to stop the worker.
		}
		select {
		case <-s.kaCtx.Done():
			log.L().Info("keepalive with master goroutine exited!")
			return
		case <-time.After(retryConnectSleepTime):
			// Try to connect master again
		}
	}
}

// UpdateKeepAliveTTL updates keepalive key with new lease TTL in place, to avoid watcher observe a DELETE event.
func (s *Server) UpdateKeepAliveTTL(newTTL int64) {
	ha.KeepAliveUpdateCh <- newTTL
	log.L().Debug("received update keepalive TTL request, should be updated soon", zap.Int64("new ttl", newTTL))
}
