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
	"github.com/pingcap/tiflow/dm/dumpling"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/engine/jobmaster/dm"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/pingcap/tiflow/engine/lib"
	"github.com/pingcap/tiflow/engine/model"
	"github.com/pingcap/tiflow/engine/pkg/p2p"
)

var _ lib.Worker = &dumpWorker{}

type dumpWorker struct {
	lib.BaseWorker

	cfg        *config.SubTaskConfig
	unitHolder *unitHolder
}

func newDumpWorker(cfg lib.WorkerConfig) lib.WorkerImpl {
	subtaskCfg := cfg.(*config.SubTaskConfig)
	return &dumpWorker{
		cfg: subtaskCfg,
	}
}

func (d *dumpWorker) InitImpl(ctx context.Context) error {
	log.L().Info("init dump worker")

	rid := dm.NewDMResourceID(d.cfg.Name, d.cfg.SourceID)
	h, err := d.OpenStorage(ctx, rid)
	for status.Code(errors.Cause(err)) == codes.Unavailable {
		// TODO: use backoff retry later
		log.L().Info("simple retry", zap.Error(err))
		time.Sleep(time.Second)
		h, err = d.OpenStorage(ctx, rid)
	}
	if err != nil {
		return errors.Trace(err)
	}
	d.cfg.ExtStorage = h.BrExternalStorage()

	d.unitHolder = newUnitHolder(lib.WorkerDMDump, d.cfg.SourceID, dumpling.NewDumpling(d.cfg))
	d.unitHolder.storageWriteHandle = h
	return errors.Trace(d.unitHolder.init(ctx))
}

func (d *dumpWorker) Tick(ctx context.Context) error {
	d.unitHolder.lazyProcess()
	return d.unitHolder.tryUpdateStatus(ctx, d.BaseWorker)
}

func (d *dumpWorker) Workload() model.RescUnit {
	log.L().Info("dumpWorker.Workload")
	return 0
}

func (d *dumpWorker) OnMasterMessage(topic p2p.Topic, message p2p.MessageValue) error {
	log.L().Info("dumpWorker.OnMasterMessage", zap.Any("message", message))
	return nil
}

func (d *dumpWorker) CloseImpl(ctx context.Context) error {
	d.unitHolder.close()
	return nil
}
