package servermaster

import (
	"context"
	"testing"

	"github.com/hanfei1991/microcosm/model"
	"github.com/hanfei1991/microcosm/pb"
	"github.com/hanfei1991/microcosm/pkg/adapter"
	"github.com/hanfei1991/microcosm/pkg/rpcutil"
	"github.com/hanfei1991/microcosm/test"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/stretchr/testify/require"
)

func init() {
	// initialized the logger to make genEmbedEtcdConfig working.
	err := log.InitLogger(&log.Config{})
	if err != nil {
		panic(err)
	}
}

func TestMembershipIface(t *testing.T) {
	t.Parallel()

	etcdName := "membership-test1"
	id := genServerMasterUUID(etcdName)
	addr, etcd, client, cleanFn := test.PrepareEtcd(t, etcdName)
	defer cleanFn()

	testCases := []struct {
		name string
		addr string
		tp   model.NodeType
	}{
		{id, addr, model.NodeTypeServerMaster},
		{"membership-executor-test1", "127.0.0.1:10000", model.NodeTypeExecutor},
	}

	ctx := context.Background()
	for _, tc := range testCases {
		info := &model.NodeInfo{
			Type: tc.tp,
			ID:   model.DeployNodeID(tc.name),
			Addr: tc.addr,
		}
		infoBytes, err := info.ToJSON()
		require.Nil(t, err)
		_, err = client.Put(ctx, adapter.NodeInfoKeyAdapter.Encode(tc.name), infoBytes)
		require.Nil(t, err)
	}

	// test Membership.GetMembers
	membership := &EtcdMembership{etcdCli: client}
	etcdLeaderID := etcd.Server.Lead()
	leader := &Member{Name: id}
	members, err := membership.GetMembers(ctx, leader, etcdLeaderID)
	require.Nil(t, err)
	require.Len(t, members, 1)
	require.Equal(t, id, members[0].Name)
}

func TestUpdateServerMembers(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	etcdName := "membership-test2"
	id := genServerMasterUUID(etcdName)
	addr, etcd, etcdCli, cleanFn := test.PrepareEtcd(t, etcdName)
	defer cleanFn()

	testCases := []struct {
		name string
		addr string
		tp   model.NodeType
	}{
		{id, addr, model.NodeTypeServerMaster},
		{"membership-executor-test1", "127.0.0.1:10000", model.NodeTypeExecutor},
	}

	for _, tc := range testCases {
		info := &model.NodeInfo{
			Type: tc.tp,
			ID:   model.DeployNodeID(tc.name),
			Addr: tc.addr,
		}
		infoBytes, err := info.ToJSON()
		require.Nil(t, err)
		_, err = etcdCli.Put(ctx, adapter.NodeInfoKeyAdapter.Encode(tc.name), infoBytes)
		require.Nil(t, err)
	}

	cfg := NewConfig()
	cfg.Etcd.Name = etcdName
	cfg.AdvertiseAddr = addr

	s := &Server{
		cfg:        cfg,
		etcd:       etcd,
		membership: &EtcdMembership{etcdCli: etcdCli},
	}
	preRPCHook := rpcutil.NewPreRPCHook[pb.MasterClient](
		id,
		&s.leader,
		s.masterCli,
		&s.leaderInitialized,
		s.rpcLogRL,
	)
	s.masterRPCHook = preRPCHook
	leader, exists := s.masterRPCHook.CheckLeader()
	require.Nil(t, leader)
	require.False(t, exists)

	member := &Member{
		Name:          id,
		IsServLeader:  true,
		IsEtcdLeader:  true,
		AdvertiseAddr: addr,
	}
	s.leader.Store(member)
	err := s.updateServerMasterMembers(ctx)
	require.Nil(t, err)
	s.members.RLock()
	defer s.members.RUnlock()
	require.Equal(t, []*Member{member}, s.members.m)
}
