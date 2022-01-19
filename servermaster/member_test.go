package servermaster

import (
	"context"
	"encoding/json"
	"testing"

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

	// prepare master info config
	ctx := context.Background()
	cfg := NewConfig()
	cfg.Etcd.Name = name
	cfg.AdvertiseAddr = addr
	cfgBytes, err := json.Marshal(cfg)
	require.Nil(t, err)
	_, err = client.Put(ctx, adapter.MasterInfoKey.Encode(name), string(cfgBytes))
	require.Nil(t, err)

	// test Membership.GetConfigs
	membership := &EtcdMembership{etcdCli: client}
	cfgs, err := membership.GetConfigs(ctx)
	require.Nil(t, err)
	require.Equal(t, 1, len(cfgs))
	require.Contains(t, cfgs, name)

	// test Membership.GetMembers
	etcdLeaderID := etcd.Server.Lead()
	leader := &Member{Name: name}
	members, err := membership.GetMembers(ctx, leader, etcdLeaderID)
	require.Nil(t, err)
	require.Len(t, members, 1)
}

func TestUpdateServerMembers(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	name := "membership-test2"
	addr, etcd, etcdCli, cleanFn := test.PrepareEtcd(t, name)
	defer cleanFn()

	cfg := NewConfig()
	cfg.Etcd.Name = name
	cfg.AdvertiseAddr = addr
	cfgBytes, err := json.Marshal(cfg)
	require.Nil(t, err)
	_, err = etcdCli.Put(ctx, adapter.MasterInfoKey.Encode(name), string(cfgBytes))
	require.Nil(t, err)

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
	err = s.updateServerMasterMembers(ctx)
	require.Nil(t, err)
	s.members.RLock()
	defer s.members.RUnlock()
	require.Equal(t, []*Member{member}, s.members.m)
}
