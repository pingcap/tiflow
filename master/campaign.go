package master

import (
	"context"

	"github.com/hanfei1991/microcosm/pkg/errors"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"go.uber.org/zap"
)

func (s *Server) campaignLeaderLoop(ctx context.Context) error {
	for {
		err := s.reset(ctx)
		if err != nil {
			return err
		}
		leaderCtx, resignFn, err := s.election.Campaign(ctx, s.name())
		switch err {
		case nil:
		case context.Canceled:
			return ctx.Err()
		default:
			log.L().Warn("campaign leader failed", zap.Error(err))
			return errors.Wrap(errors.ErrMasterCampaignLeader, err)
		}
		s.resignFn = resignFn
		// TODO: if etcd leader is different with current server, resign current
		// leader to keep them same
		log.L().Info("campaign leader successfully", zap.String("server-id", s.name()))
		cctx, cancel := context.WithCancel(leaderCtx)
		err = s.runLeaderService(cctx)
		cancel()
		log.L().Info("leader resigned", zap.Error(err))
	}
}

func (s *Server) resign() {
	s.resignFn()
}
