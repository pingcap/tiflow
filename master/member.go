package master

import (
	"context"
	"encoding/json"

	"github.com/hanfei1991/microcosm/pkg/adapter"
	"github.com/hanfei1991/microcosm/pkg/errors"
	"github.com/hanfei1991/microcosm/pkg/etcdutils"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"go.etcd.io/etcd/clientv3/concurrency"
	"go.uber.org/zap"
)

type Member struct {
	IsServLeader bool
	IsEtcdLeader bool
	Name         string
	Addrs        []string
}

// TODO: we can watch leader election to observe leader change, and don't need
// to list members
func (s *Server) updateServerMasterMembers(ctx context.Context) error {
	leader, err := etcdutils.GetLeaderID(ctx, s.etcdClient, adapter.MasterCampaignKey.Path())
	if err != nil {
		if err == concurrency.ErrElectionNoLeader {
			log.L().Warn("etcd election no leader")
		} else {
			return err
		}
	}
	if leader != "" {
		resp, err := s.etcdClient.Get(ctx, adapter.MasterInfoKey.Encode(leader))
		if err != nil {
			return errors.Wrap(errors.ErrEtcdAPIError, err)
		}
		if resp.Count > 0 {
			cfg := &Config{}
			err = json.Unmarshal(resp.Kvs[0].Value, cfg)
			if err != nil {
				return err
			}
			leader = cfg.Etcd.Name
		}
	}
	resp, err := s.etcdClient.MemberList(ctx)
	if err != nil {
		return err
	}
	members := make([]*Member, 0, len(resp.Members))
	currLeader, exist := s.checkLeader()
	for _, m := range resp.Members {
		isServLeader := m.Name == leader
		if isServLeader && (!exist || currLeader.Name != m.Name) {
			s.leader.Store(&Member{
				Name:         m.Name,
				IsServLeader: true,
				IsEtcdLeader: true,
				Addrs:        m.ClientURLs,
			})
			s.createLeaderClient(ctx, m.ClientURLs)
		}
		members = append(members, &Member{
			Name:         m.Name,
			Addrs:        m.ClientURLs,
			IsEtcdLeader: false, /* TODO */
			IsServLeader: isServLeader,
		})
	}
	s.members = members
	log.L().Info("update members", zap.Any("members", members))
	return nil
}
