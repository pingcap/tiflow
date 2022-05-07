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
	"github.com/pingcap/tiflow/dm/loader"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/engine/jobmaster/dm"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/pingcap/tiflow/engine/lib"
	"github.com/pingcap/tiflow/engine/model"
	"github.com/pingcap/tiflow/engine/pkg/p2p"
)

var _ lib.Worker = &loadWorker{}

type loadWorker struct {
	lib.BaseWorker

	cfg        *config.SubTaskConfig
	unitHolder *unitHolder
}

func newLoadWorker(cfg lib.WorkerConfig) lib.WorkerImpl {
	subtaskCfg := cfg.(*config.SubTaskConfig)
	return &loadWorker{
		cfg: subtaskCfg,
	}
}

func (l *loadWorker) InitImpl(ctx context.Context) error {
	log.L().Info("init load worker")

	rid := dm.NewDMResourceID(l.cfg.Name, l.cfg.SourceID)
	h, err := l.OpenStorage(ctx, rid)
	for status.Code(err) == codes.Unavailable {
		log.L().Info("simple retry", zap.Error(err))
		time.Sleep(time.Second)
		h, err = l.OpenStorage(ctx, rid)
	}
	if err != nil {
		return errors.Trace(err)
	}
	l.cfg.ExtStorage = h.BrExternalStorage()

	// `workerName` and `etcdClient` of `NewLightning` are not used in dataflow
	// scenario, we just use readable values here.
	workerName := "dataflow-worker"
	l.unitHolder = newUnitHolder(lib.WorkerDMLoad, l.cfg.SourceID, loader.NewLightning(l.cfg, nil, workerName))
	return errors.Trace(l.unitHolder.init(ctx))
}

func (l *loadWorker) Tick(ctx context.Context) error {
	l.unitHolder.lazyProcess()
	return l.unitHolder.tryUpdateStatus(ctx, l.BaseWorker)
}

func (l *loadWorker) Workload() model.RescUnit {
	log.L().Info("loadWorker.Workload")
	return 0
}

func (l *loadWorker) OnMasterFailover(reason lib.MasterFailoverReason) error {
	log.L().Info("loadWorker.OnMasterFailover")
	return nil
}

func (l *loadWorker) OnMasterMessage(topic p2p.Topic, message p2p.MessageValue) error {
	log.L().Info("loadWorker.OnMasterMessage", zap.Any("message", message))
	return nil
}

func (l *loadWorker) CloseImpl(ctx context.Context) error {
	l.unitHolder.close()
	return nil
}
