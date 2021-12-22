package master

import (
	"context"
	"strings"

	"github.com/hanfei1991/microcosm/client"
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

func (s *Server) createLeaderClient(ctx context.Context, addrs []string) {
	s.closeLeaderClient()

	endpoints := make([]string, 0, len(addrs))
	for _, addr := range addrs {
		endpoints = append(endpoints, strings.Replace(addr, "http://", "", 1))
	}
	cli, err := client.NewMasterClient(ctx, endpoints)
	if err != nil {
		log.L().Error("create server master client failed", zap.Strings("addrs", addrs), zap.Error(err))
		return
	}
	s.leaderClient = cli
}

func (s *Server) closeLeaderClient() {
	if s.leaderClient != nil {
		err := s.leaderClient.Close()
		if err != nil {
			log.L().Warn("close leader client met error", zap.Error(err))
		}
		s.leaderClient = nil
	}
}
