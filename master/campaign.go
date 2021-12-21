package master

import (
	"context"
	"time"

	"github.com/hanfei1991/microcosm/pkg/errors"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"go.etcd.io/etcd/mvcc"
	"go.uber.org/zap"
	"golang.org/x/time/rate"
)

func (s *Server) campaignLeaderLoop(ctx context.Context) error {
	rl := rate.NewLimiter(rate.Every(time.Millisecond*200), 1)
	for {
		err := rl.Wait(ctx)
		if err != nil {
			return err
		}
		err = s.reset(ctx)
		if err != nil {
			return err
		}
		err = s.campaign(ctx)
		switch err {
		case nil:
		case context.Canceled:
			return ctx.Err()
		case mvcc.ErrCompacted:
			continue
		default:
			log.L().Warn("campaign leader failed", zap.Error(err))
			return errors.Wrap(errors.ErrMasterCampaignLeader, err)
		}
		// TODO: if etcd leader is different with current server, resign current
		// leader to keep them same
		log.L().Info("campaign leader successfully", zap.String("server-id", s.name()))
		cctx, cancel := context.WithCancel(ctx)
		err = s.runLeaderService(cctx)
		cancel()
		return err
	}
}

func (s *Server) campaign(ctx context.Context) error {
	err := s.election.Campaign(ctx, s.name())
	return errors.Wrap(errors.ErrMasterCampaignLeader, err)
}

func (s *Server) resign() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	err := s.election.Resign(ctx)
	if err != nil {
		log.L().Warn("resign leader failed", zap.Error(err))
	}
}
