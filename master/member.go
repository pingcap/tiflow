package master

import (
	"context"
	"encoding/json"

	"github.com/pingcap/tiflow/dm/pkg/log"
	"go.uber.org/zap"
)

// Member stores server member information
// TODO: make it a protobuf field and can be shared by gRPC API
type Member struct {
	IsServLeader bool     `json:"is-serv-leader"`
	IsEtcdLeader bool     `json:"is-etcd-leader"`
	Name         string   `json:"name"`
	Addrs        []string `json:"addrs"`
}

// String implements json marshal
func (m *Member) String() (string, error) {
	b, err := json.Marshal(m)
	return string(b), err
}

// Unmarshal unmarshals data into a member
func (m *Member) Unmarshal(data []byte) error {
	return json.Unmarshal(data, m)
}

func (s *Server) updateServerMasterMembers(ctx context.Context) error {
	resp, err := s.etcdClient.MemberList(ctx)
	if err != nil {
		return err
	}

	members := make([]*Member, 0, len(resp.Members))
	leader, exists := s.checkLeader()
	etcdLeaderID := s.etcd.Server.Lead()
	for _, m := range resp.Members {
		isServLeader := exists && m.Name == leader.Name
		isEtcdLeader := m.ID == etcdLeaderID
		members = append(members, &Member{
			Name:         m.Name,
			Addrs:        m.ClientURLs,
			IsEtcdLeader: isEtcdLeader,
			IsServLeader: isServLeader,
		})
	}
	s.members.Lock()
	defer s.members.Unlock()
	s.members.m = members
	log.L().Info("update server master members", zap.Any("members", members))
	return nil
}
