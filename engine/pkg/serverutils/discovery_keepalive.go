// Copyright 2022 PingCAP, Inc.
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

package serverutils

import (
	"context"
	"time"

	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/engine/model"
	"github.com/pingcap/tiflow/engine/pkg/srvdiscovery"
	"github.com/pingcap/tiflow/pkg/p2p"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

// DiscoveryKeepaliver wraps wraps DiscoveryRunner and MessageRouter
type DiscoveryKeepaliver struct {
	info       *model.NodeInfo
	etcdCli    *clientv3.Client
	sessionTTL int
	watchDur   time.Duration

	discoveryRunner     srvdiscovery.DiscoveryRunner
	initDiscoveryRunner func() error
	p2pMsgRouter        p2p.MessageRouter
}

// NewDiscoveryKeepaliver creates a new DiscoveryKeepaliver
func NewDiscoveryKeepaliver(
	info *model.NodeInfo,
	etcdCli *clientv3.Client,
	sessionTTL int,
	watchDur time.Duration,
	msgRouter p2p.MessageRouter,
) *DiscoveryKeepaliver {
	k := &DiscoveryKeepaliver{
		info:         info,
		etcdCli:      etcdCli,
		sessionTTL:   sessionTTL,
		watchDur:     watchDur,
		p2pMsgRouter: msgRouter,
	}
	k.initDiscoveryRunner = k.InitRunnerImpl
	return k
}

// InitRunnerImpl inits the discovery runner
func (k *DiscoveryKeepaliver) InitRunnerImpl() error {
	value, err := k.info.ToJSON()
	if err != nil {
		return err
	}

	k.discoveryRunner = srvdiscovery.NewDiscoveryRunnerImpl(
		k.etcdCli, k.sessionTTL, k.watchDur, k.info.EtcdKey(), value)
	return nil
}

// Keepalive keeps discovery runner running, watches peer changes and applies
// peer changes to message router.
func (k *DiscoveryKeepaliver) Keepalive(ctx context.Context) error {
	var (
		session srvdiscovery.Session
		err     error
	)

	err = k.initDiscoveryRunner()
	if err != nil {
		return err
	}
	session, err = k.discoveryRunner.ResetDiscovery(ctx, true /* resetSession*/)
	if err != nil {
		return err
	}
	executors := k.discoveryRunner.GetSnapshot()
	for uuid, exec := range executors {
		if k.p2pMsgRouter != nil {
			log.L().Info("add peer",
				zap.String("uuid", uuid),
				zap.Any("exec", exec))
			k.p2pMsgRouter.AddPeer(uuid, exec.Addr)
		}
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-session.Done():
			log.L().Warn("metastore session is done", zap.String("executor-id", string(k.info.ID)))
			session, err = k.discoveryRunner.ResetDiscovery(ctx, true /* resetSession*/)
			if err != nil {
				return err
			}
		case resp := <-k.discoveryRunner.GetWatcher():
			if resp.Err != nil {
				log.L().Warn("discovery watch met error", zap.Error(resp.Err))
				_, err = k.discoveryRunner.ResetDiscovery(ctx, false /* resetSession*/)
				if err != nil {
					return err
				}
				continue
			}
			for uuid, add := range resp.AddSet {
				if k.p2pMsgRouter != nil {
					log.L().Info("add peer",
						zap.String("uuid", uuid),
						zap.Any("exec", add))
					k.p2pMsgRouter.AddPeer(uuid, add.Addr)
				}
			}
			for uuid := range resp.DelSet {
				if k.p2pMsgRouter != nil {
					log.L().Info("remove peer",
						zap.String("uuid", uuid))
					k.p2pMsgRouter.RemovePeer(uuid)
				}
			}
			k.discoveryRunner.ApplyWatchResult(resp)
		}
	}
}
