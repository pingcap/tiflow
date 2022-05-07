// Copyright 2022 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package dm

import (
	"context"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/dm/dm/config"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/dm/syncer"
	"github.com/pingcap/tiflow/engine/jobmaster/dm"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/pingcap/tiflow/engine/lib"
	"github.com/pingcap/tiflow/engine/model"
	"github.com/pingcap/tiflow/engine/pkg/p2p"
)

var _ lib.Worker = &syncWorker{}

type syncWorker struct {
	lib.BaseWorker

	cfg        *config.SubTaskConfig
	unitHolder *unitHolder
}

func newSyncWorker(cfg lib.WorkerConfig) lib.WorkerImpl {
	subtaskCfg := cfg.(*config.SubTaskConfig)
	return &syncWorker{
		cfg: subtaskCfg,
	}
}

func (s *syncWorker) InitImpl(ctx context.Context) error {
	log.L().Info("init sync worker")

	if s.cfg.Mode == config.ModeAll {
		rid := dm.NewDMResourceID(s.cfg.Name, s.cfg.SourceID)
		h, err := s.OpenStorage(ctx, rid)
		for status.Code(err) == codes.Unavailable {
			log.L().Info("simple retry", zap.Error(err))
			time.Sleep(time.Second)
			h, err = s.OpenStorage(ctx, rid)
		}
		if err != nil {
			return errors.Trace(err)
		}
		s.cfg.ExtStorage = h.BrExternalStorage()
	}

	s.unitHolder = newUnitHolder(lib.WorkerDMSync, s.cfg.SourceID, syncer.NewSyncer(s.cfg, nil, nil))
	return errors.Trace(s.unitHolder.init(ctx))
}

func (s *syncWorker) Tick(ctx context.Context) error {
	s.unitHolder.lazyProcess()
	return s.unitHolder.tryUpdateStatus(ctx, s.BaseWorker)
}

func (s *syncWorker) Workload() model.RescUnit {
	log.L().Info("syncWorker.Workload")
	return 0
}

func (s *syncWorker) OnMasterFailover(reason lib.MasterFailoverReason) error {
	log.L().Info("syncWorker.OnMasterFailover")
	return nil
}

func (s *syncWorker) OnMasterMessage(topic p2p.Topic, message p2p.MessageValue) error {
	log.L().Info("syncWorker.OnMasterMessage", zap.Any("message", message))
	return nil
}

func (s *syncWorker) CloseImpl(ctx context.Context) error {
	s.unitHolder.close()
	return nil
}
