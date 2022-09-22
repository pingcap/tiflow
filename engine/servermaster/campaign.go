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
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/engine/pkg/adapter"
	"github.com/pingcap/tiflow/engine/pkg/etcdutil"
	"github.com/pingcap/tiflow/engine/pkg/rpcutil"
	"github.com/pingcap/tiflow/engine/servermaster/cluster"
	derrors "github.com/pingcap/tiflow/pkg/errors"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func (s *Server) generateSessionConfig() (*cluster.EtcdSessionConfig, error) {
	value, err := s.info.ToJSON()
	if err != nil {
		return nil, derrors.WrapError(derrors.ErrMasterNewServer, err)
	}

	return &cluster.EtcdSessionConfig{
		Member:       s.member(),
		Key:          s.info.EtcdKey(),
		Value:        value,
		KeepaliveTTL: s.cfg.KeepAliveTTL,
	}, nil
}

func (s *Server) leaderLoop(ctx context.Context) error {
	var (
		retryInterval    = time.Millisecond * 200
		needResetSession = false

		// After a leader is resigned, the server will stop to campaign itself
		// for 10 seconds to avoid the server becoming a leader again.
		stopCampaignInterval = time.Second * 10
		stopCampaignUntil    = time.Now()
	)

	sessionCfg, err := s.generateSessionConfig()
	if err != nil {
		return err
	}
	session, err := cluster.NewEtcdSession(ctx, s.etcdClient, sessionCfg)
	if err != nil {
		return err
	}

	for {
		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
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

		if time.Now().Before(stopCampaignUntil) {
			time.Sleep(retryInterval)
			continue
		}

		if needResetSession {
			if err := session.Reset(ctx); err != nil {
				return err
			}
			needResetSession = false
		}
		leaderCtx, resignFn, err := session.Campaign(ctx, defaultCampaignTimeout)
		if err != nil {
			needResetSession = session.CheckNeedReset(err)
			continue
		}

		g, gCtx := errgroup.WithContext(leaderCtx)

		g.Go(func() error {
			return s.leaderServiceFn(gCtx)
		})

		g.Go(func() error {
			var retErr error
			select {
			case <-gCtx.Done():
				retErr = gCtx.Err()
			case <-s.resignCh:
				retErr = derrors.ErrLeaderResigned.GenWithStackByArgs()
			}
			resignFn()
			return retErr
		})

		if err := g.Wait(); err != nil {
			if errors.Cause(err) == context.Canceled {
				log.Info("leader service exits", zap.Error(err))
			} else if derrors.ErrLeaderResigned.Equal(err) {
				log.Info("leader has resigned")
				stopCampaignUntil = time.Now().Add(stopCampaignInterval)
			} else if derrors.ErrMasterSessionDone.Equal(err) {
				log.Info("server master session done, reset session now", zap.Error(err))
				needResetSession = true
			} else {
				log.Error("run leader service failed", zap.Error(err))
			}
		}
	}
}

// checkLeaderExists is the entrance of server master leader loop. It works as follows
//  1. Try to query leader node from etcd
//  2. If leader node exists, decode the leader information
//     a. If decode fails, return retry=true to retry the leader loop
//     b. If leader information is stale, try to delete it and retry the leader loop
//     c. Otherwise, watch the leader until it is evicted.
func (s *Server) checkLeaderExists(ctx context.Context) (retry bool, err error) {
	// step-1
	key, data, rev, err := etcdutil.GetLeader(ctx, s.etcdClient, adapter.MasterCampaignKey.Path())
	if err != nil {
		if errors.Cause(err) == context.Canceled {
			return false, errors.Trace(err)
		}
		if !derrors.ErrMasterNoLeader.Equal(err) {
			log.Warn("get leader failed", zap.Error(err))
			return true, nil
		}
	}

	// step-2
	var leader *rpcutil.Member
	if len(data) > 0 {
		leader = &rpcutil.Member{}
		err = leader.Unmarshal(data)
		// step-2, case-a
		if err != nil {
			log.Warn("unexpected leader data", zap.Error(err))
			return true, nil
		}

		// step-2, case-b
		if leader.Name == s.name() || leader.AdvertiseAddr == s.cfg.AdvertiseAddr {
			// - leader.Name == s.name() means this server should be leader
			// - leader.AdvertiseAddr == s.cfg.AdvertiseAddr means the old leader
			//   has the same advertise addr as current server
			// Both of these two conditions indicate the existing information
			// of leader campaign is stale, just delete it and campaign again.
			log.Warn("found stale leader key, delete it and campaign later",
				zap.ByteString("key", key), zap.ByteString("val", data))
			_, err := s.etcdClient.Delete(ctx, string(key))
			if err != nil {
				log.Error("failed to delete leader key",
					zap.ByteString("key", key), zap.ByteString("val", data))
			}
			return true, nil
		}

		// step-2, case-c
		leader.IsLeader = true
		log.Info("start to watch server master leader",
			zap.String("leader-name", leader.Name), zap.String("addr", leader.AdvertiseAddr))
		s.watchLeader(ctx, leader, key, rev)
		log.Info("server master leader changed")
	}

	return false, nil
}

// TODO: we can use UpdateClients, don't need to close and re-create it.
func (s *Server) createLeaderClient(ctx context.Context, addr string) {
	s.closeLeaderClient()

	// TODO support TLS
	credentials := insecure.NewCredentials()
	conn, err := grpc.Dial(
		addr,
		grpc.WithTransportCredentials(credentials),
	)
	if err != nil {
		log.Error("create server master client failed", zap.String("addr", addr), zap.Error(err))
		return
	}
	cli := newMultiClient(conn)
	s.masterCli.Set(cli, func() {
		if err := conn.Close(); err != nil {
			log.Warn("failed to close clinet", zap.String("addr", addr))
		}
	})
}

func (s *Server) closeLeaderClient() {
	s.masterCli.Close()
}

func (s *Server) watchLeader(ctx context.Context, m *rpcutil.Member, key []byte, rev int64) {
	m.IsLeader = true
	s.leader.Store(m)

	s.createLeaderClient(ctx, m.AdvertiseAddr)
	defer s.leader.Store(&rpcutil.Member{})

	watcher := clientv3.NewWatcher(s.etcdClient)
	defer watcher.Close()
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		rch := watcher.Watch(ctx, string(key), clientv3.WithRev(rev))
		for wresp := range rch {
			if wresp.CompactRevision != 0 {
				log.Warn("watch leader met compacted error",
					zap.Int64("required-revision", rev),
					zap.Int64("compact-revision", wresp.CompactRevision),
				)
				rev = wresp.CompactRevision
				break
			}
			if wresp.Canceled {
				log.Warn("leadership watcher is canceled",
					zap.Int64("revision", rev), zap.String("leader-name", m.Name),
					zap.Error(wresp.Err()))
				return
			}
			for _, ev := range wresp.Events {
				if ev.Type == mvccpb.DELETE {
					log.Info("current leader is resigned", zap.String("leader-name", m.Name))
					return
				}
			}
		}
	}
}

// watchNewLeader is like watchLeader, but it uses the new elector interface.
// TODO:
//  1. Remove all the legacy code based on etcd.
//  2. Integrate elector, masterRPCHook, masterCli, s.leader.
func (s *Server) watchNewLeader(ctx context.Context) error {
	ticker := time.NewTicker(time.Millisecond * 100)
	defer ticker.Stop()

	var leaderAddr string
	for {
		select {
		case <-ticker.C:
			leader, ok := s.elector.GetLeader()
			if ok {
				s.leader.Store(&rpcutil.Member{
					Name:          leader.ID, // FIXME: rpcutil.Member.Name use id as name which is confusing.
					AdvertiseAddr: leader.Address,
					IsLeader:      true,
				})
				if leader.Address != leaderAddr {
					leaderAddr = leader.Address
					s.createLeaderClient(ctx, leaderAddr)
				}
			} else {
				s.leader.Store(&rpcutil.Member{})
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
