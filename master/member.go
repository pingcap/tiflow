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
	IsServLeader bool     `json:"-"`
	IsEtcdLeader bool     `json:"-"`
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

// nolint:unused
func (s *Server) updateServerMasterMembers(ctx context.Context) error {
	leader, exists := s.checkLeader()
	resp, err := s.etcdClient.MemberList(ctx)
	if err != nil {
		return err
	}
	members := make([]*Member, 0, len(resp.Members))
	currLeader, exist := s.checkLeader()
	for _, m := range resp.Members {
		isServLeader := exists && m.Name == leader.Name
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
