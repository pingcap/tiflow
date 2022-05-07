package servermaster

import (
	"context"
	"encoding/json"

	"github.com/hanfei1991/microcosm/model"
	"github.com/hanfei1991/microcosm/pkg/adapter"
	"github.com/hanfei1991/microcosm/pkg/errors"
	"github.com/hanfei1991/microcosm/pkg/rpcutil"
	"github.com/pingcap/tiflow/dm/pkg/log"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

// uuid length = 36, plus one "-" symbol
const idSuffixLen = 37

type Member = rpcutil.Member

// Membership defines the interface to query member information in metastore
type Membership interface {
	GetMembers(ctx context.Context, leader *Member, etcdLeaderID uint64) ([]*Member, error)
}

type EtcdMembership struct {
	etcdCli *clientv3.Client
}

func (em *EtcdMembership) getMasterNodes(ctx context.Context) (map[string]*model.NodeInfo, error) {
	resp, err := em.etcdCli.Get(ctx, adapter.NodeInfoKeyAdapter.Path(), clientv3.WithPrefix())
	if err != nil {
		return nil, errors.Wrap(errors.ErrEtcdAPIError, err)
	}
	nodes := make(map[string]*model.NodeInfo, resp.Count)
	for _, kv := range resp.Kvs {
		info := &model.NodeInfo{}
		err := json.Unmarshal(kv.Value, info)
		if err != nil {
			return nil, errors.Wrap(errors.ErrDecodeEtcdKeyFail, err)
		}
		if info.Type == model.NodeTypeServerMaster {
			id := string(info.ID)
			if len(id) < idSuffixLen {
				return nil, errors.ErrInvalidServerMasterID.GenWithStackByArgs(id)
			}
			nodes[id[:len(id)-idSuffixLen]] = info
		}
	}
	return nodes, nil
}

func (em *EtcdMembership) GetMembers(ctx context.Context, leader *Member, etcdLeaderID uint64) ([]*Member, error) {
	servers, err := em.getMasterNodes(ctx)
	if err != nil {
		return nil, err
	}

	etcdMembers, err := em.etcdCli.MemberList(ctx)
	if err != nil {
		return nil, err
	}

	members := make([]*Member, 0, len(etcdMembers.Members))
	for _, m := range etcdMembers.Members {
		server, ok := servers[m.Name]
		if !ok {
			continue
		}
		isServLeader := leader != nil && string(server.ID) == leader.Name
		isEtcdLeader := m.ID == etcdLeaderID
		members = append(members, &Member{
			Name:          string(server.ID),
			AdvertiseAddr: server.Addr,
			IsEtcdLeader:  isEtcdLeader,
			IsServLeader:  isServLeader,
		})
	}
	return members, nil
}

func (s *Server) updateServerMasterMembers(ctx context.Context) error {
	leader, exists := s.masterRPCHook.CheckLeader()
	if !exists {
		leader = nil
	}
	etcdLeaderID := s.etcd.Server.Lead()
	members, err := s.membership.GetMembers(ctx, leader, etcdLeaderID)
	if err != nil {
		return err
	}
	s.members.Lock()
	defer s.members.Unlock()
	s.members.m = members
	log.L().Info("update server master members", zap.Any("members", members))
	return nil
}
