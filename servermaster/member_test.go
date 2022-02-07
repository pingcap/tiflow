package servermaster

import (
	"context"
	"testing"

	"github.com/hanfei1991/microcosm/model"
	"github.com/hanfei1991/microcosm/pkg/adapter"
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

	name := "membership-test1"
	addr, etcd, client, cleanFn := test.PrepareEtcd(t, name)
	defer cleanFn()

	testCases := []struct {
		name string
		addr string
		tp   model.NodeType
	}{
		{name, addr, model.NodeTypeServerMaster},
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
	leader := &Member{Name: name}
	members, err := membership.GetMembers(ctx, leader, etcdLeaderID)
	require.Nil(t, err)
	require.Len(t, members, 1)
	require.Equal(t, name, members[0].Name)
}

func TestUpdateServerMembers(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	name := "membership-test2"
	addr, etcd, etcdCli, cleanFn := test.PrepareEtcd(t, name)
	defer cleanFn()

	testCases := []struct {
		name string
		addr string
		tp   model.NodeType
	}{
		{name, addr, model.NodeTypeServerMaster},
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
	cfg.Etcd.Name = name
	cfg.AdvertiseAddr = addr

	s := &Server{
		cfg:        cfg,
		etcd:       etcd,
		membership: &EtcdMembership{etcdCli: etcdCli},
	}
	leader, exists := s.checkLeader()
	require.Nil(t, leader)
	require.False(t, exists)

	member := &Member{
		Name:          name,
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
