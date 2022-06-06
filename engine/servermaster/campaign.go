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

package servermaster

import (
	"context"
	"strings"
	"time"

	perrors "github.com/pingcap/errors"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"

	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/engine/client"
	"github.com/pingcap/tiflow/engine/pkg/adapter"
	"github.com/pingcap/tiflow/engine/pkg/errors"
	"github.com/pingcap/tiflow/engine/pkg/etcdutils"
	"github.com/pingcap/tiflow/engine/pkg/externalresource/manager"
	"github.com/pingcap/tiflow/engine/servermaster/cluster"
)

func (s *Server) leaderLoop(ctx context.Context) error {
	var (
		leaderCtx      context.Context
		leaderResignFn context.CancelFunc

		retryInterval    = time.Millisecond * 200
		needResetSession = false
	)
	defer func() {
		if leaderResignFn != nil {
			leaderResignFn()
		}
	}()

	session, err := cluster.NewEtcdSession(ctx, s.etcdClient, s.member(),
		s.info, s.cfg.RPCTimeout, s.cfg.KeepAliveTTL)
	if err != nil {
		return err
	}

	for {
		select {
		case <-ctx.Done():
			return perrors.Trace(ctx.Err())
		default:
		}

		retryLeaderLoop, err := s.checkLeaderExists(ctx)
		if err != nil {
			return err
		}
		if retryLeaderLoop {
			time.Sleep(retryInterval)
			continue
		}

		if needResetSession {
			if err := session.Reset(ctx); err != nil {
				return err
			}
			needResetSession = false
		}
		newCtx, newResignFn, err := session.Campaign(ctx, defaultCampaignTimeout)
		if err != nil {
			needResetSession = session.CheckNeedReset(err)
			continue
		}
		leaderCtx = newCtx
		leaderResignFn = newResignFn

		err = s.leaderServiceFn(leaderCtx)
		if err != nil {
			if perrors.Cause(err) == context.Canceled ||
				errors.ErrEtcdLeaderChanged.Equal(err) {
				log.L().Info("leader service exits", zap.Error(err))
			} else if errors.ErrMasterSessionDone.Equal(err) {
				log.L().Info("server master session done, reset session now", zap.Error(err))
				needResetSession = true
			} else {
				log.L().Error("run leader service failed", zap.Error(err))
			}
		}
	}
}

// checkLeaderExists is the entrance of server master leader loop. It works as follows
// 1. Try to query leader node from etcd
// 2. If leader node exists, decode the leader information
//    - If decode fails, return retry=true to retry the leader loop
//    - If leader information is stale, try to delete it and retry the leader loop
//    - Otherwise watch the leader until it is evicted.
// 3. If the leader doesn't exist, check whether current node is etcd leader
//    - If it is not, return retry=true to retry the leader loop
//    - If it is, return retry=false and continue the leader campaign.
func (s *Server) checkLeaderExists(ctx context.Context) (retry bool, err error) {
	key, data, rev, err := etcdutils.GetLeader(ctx, s.etcdClient, adapter.MasterCampaignKey.Path())
	if err != nil {
		if perrors.Cause(err) == context.Canceled {
			return false, perrors.Trace(err)
		}
		if !errors.ErrMasterNoLeader.Equal(err) {
			log.L().Warn("get leader failed", zap.Error(err))
			return true, nil
		}
	}
	var leader *Member
	if len(data) > 0 {
		leader = &Member{}
		err = leader.Unmarshal(data)
		if err != nil {
			log.L().Warn("unexpected leader data", zap.Error(err))
			return true, nil
		}
		if leader.Name == s.name() || leader.AdvertiseAddr == s.cfg.AdvertiseAddr {
			// - leader.Name == s.name() means this server should be leader
			// - leader.AdvertiseAddr == s.cfg.AdvertiseAddr means the old leader
			//   has the same advertise addr as current server
			// Both of these two conditions indicate the existing information
			// of leader campaign is stale, just delete it and campaign again.
			log.L().Warn("found stale leader key, delete it and campaign later",
				zap.ByteString("key", key), zap.ByteString("val", data))
			_, err := s.etcdClient.Delete(ctx, string(key))
			if err != nil {
				log.L().Error("failed to delete leader key",
					zap.ByteString("key", key), zap.ByteString("val", data))
			}
			return true, nil
		}
		leader.IsServLeader = true
		leader.IsEtcdLeader = true
	}
	if leader != nil {
		log.L().Info("start to watch server master leader",
			zap.String("leader-name", leader.Name), zap.String("addr", leader.AdvertiseAddr))
		s.watchLeader(ctx, leader, rev)
		log.L().Info("server master leader changed")
	}
	if !s.isEtcdLeader() {
		log.L().Info("skip campaigning leader", zap.String("name", s.name()))
		return true, nil
	}
	return false, nil
}

// TODO: we can use UpdateClients, don't need to close and re-create it.
func (s *Server) createLeaderClient(ctx context.Context, addrs []string) {
	s.closeLeaderClient()

	endpoints := make([]string, 0, len(addrs))
	for _, addr := range addrs {
		endpoints = append(endpoints, strings.Replace(addr, "http://", "", 1))
	}
	cli, err := client.NewMasterClient(ctx, endpoints)
	if err != nil {
		log.L().Error("create server master client failed", zap.Strings("addrs", addrs), zap.Error(err))
		return
	}
	s.masterCli.Set(cli.FailoverRPCClients)

	cli2, err := manager.NewResourceClient(ctx, endpoints)
	if err != nil {
		log.L().Error("create resource client failed", zap.Strings("addrs", addrs), zap.Error(err))
		return
	}
	s.resourceCli.Set(cli2)
}

func (s *Server) closeLeaderClient() {
	s.masterCli.Close()
	s.resourceCli.Close()
}

func (s *Server) isEtcdLeader() bool {
	return s.etcd.Server.Lead() == uint64(s.etcd.Server.ID())
}

func (s *Server) watchLeader(ctx context.Context, m *Member, rev int64) {
	m.IsServLeader = true
	m.IsEtcdLeader = true
	s.leader.Store(m)
	s.createLeaderClient(ctx, []string{m.AdvertiseAddr})
	defer s.leader.Store(&Member{})

	watcher := clientv3.NewWatcher(s.etcdClient)
	defer watcher.Close()
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		leaderPath := adapter.MasterCampaignKey.Path()
		rch := watcher.Watch(ctx, leaderPath, clientv3.WithPrefix(), clientv3.WithRev(rev))
		for wresp := range rch {
			if wresp.CompactRevision != 0 {
				log.L().Warn("watch leader met compacted error",
					zap.Int64("required-revision", rev),
					zap.Int64("compact-revision", wresp.CompactRevision),
				)
				rev = wresp.CompactRevision
				break
			}
			if wresp.Canceled {
				log.L().Warn("leadership watcher is canceled",
					zap.Int64("revision", rev), zap.String("leader-name", m.Name),
					zap.Error(wresp.Err()))
				return
			}
			for _, ev := range wresp.Events {
				if ev.Type == mvccpb.DELETE {
					log.L().Info("current leader is resigned", zap.String("leader-name", m.Name))
					return
				}
			}
		}
	}
}
