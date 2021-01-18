// Copyright 2021 PingCAP, Inc.
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

package processor

import (
	"context"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/cenkalti/backoff"
	"github.com/juju/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cdc/kv"
	"github.com/pingcap/ticdc/cdc/model"
	cdccontext "github.com/pingcap/ticdc/pkg/context"
	cerror "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/notify"
	"github.com/pingcap/ticdc/pkg/orchestrator"
	tablepipeline "github.com/pingcap/ticdc/pkg/processor/pipeline"
	"github.com/pingcap/ticdc/pkg/retry"
	"github.com/pingcap/ticdc/pkg/util"
	"github.com/tikv/client-go/txnkv/oracle"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/mvcc"
	"go.uber.org/zap"
)

//resolvedTsGauge := resolvedTsGauge.WithLabelValues(p.changefeedID, p.captureInfo.AdvertiseAddr)
//metricResolvedTsLagGauge := resolvedTsLagGauge.WithLabelValues(p.changefeedID, p.captureInfo.AdvertiseAddr)
//checkpointTsGauge := checkpointTsGauge.WithLabelValues(p.changefeedID, p.captureInfo.AdvertiseAddr)
//metricCheckpointTsLagGauge := checkpointTsLagGauge.WithLabelValues(p.changefeedID, p.captureInfo.AdvertiseAddr)
type Processor struct {
	changefeed changefeedState

	tables map[model.TableID]*tablepipeline.TablePipeline
}

func (p *Processor) Tick(ctx context.Context, state changefeedState) (nextState orchestrator.ReactorState, err error) {
	p.changefeed = state
	// TODO check stopped changefeed
	if err := p.handleTableOperation(ctx); err != nil {
		return nil, errors.Trace(err)
	}
	if err := p.initTables(ctx); err != nil {
		return nil, errors.Trace(err)
	}

	// TODO calculate position
	return p.changefeed, nil
}

func (p *Processor) handleTableOperation(ctx context.Context) error {
	taskStatus := p.changefeed.taskStatus.Clone()
	taskStatusChanged := false
	for tableID, opt := range taskStatus.Operation {
		if opt.TableApplied() {
			continue
		}
		localCheckpointTs := p.changefeed.taskPosition.CheckPointTs
		if opt.Delete {
			if opt.BoundaryTs <= localCheckpointTs {
				table, exist := p.tables[tableID]
				if !exist {
					log.Warn("table which will be deleted is not found",
						util.ZapFieldChangefeed(ctx), zap.Int64("tableID", tableID))
					opt.Status = model.OperFinished
					opt.Done = true
					taskStatusChanged = true
					continue
				}
				switch opt.Status {
				case model.OperDispatched:
					table.AsyncStop()
					opt.Status = model.OperProcessed
					taskStatusChanged = true
				case model.OperProcessed:
					if table.Status() == tablepipeline.TableStatusStopped {
						opt.BoundaryTs = table.CheckpointTs()
						opt.Status = model.OperFinished
						opt.Done = true
						taskStatusChanged = true
					}
					delete(p.tables, tableID)
					log.Debug("Operation done signal received",
						util.ZapFieldChangefeed(ctx),
						zap.Int64("tableID", tableID),
						zap.Reflect("operation", opt))
				default:
					log.Panic("unreachable")
				}
			}
		} else {
			switch opt.Status {
			case model.OperDispatched:
				replicaInfo, exist := p.changefeed.taskStatus.Tables[tableID]
				if !exist {
					return cerror.ErrProcessorTableNotFound.GenWithStack("replicaInfo of table(%d)", tableID))
				}
				if p.changefeed.info.Config.Cyclic.IsEnabled() && replicaInfo.MarkTableID == 0 {
					return cerror.ErrProcessorTableNotFound.GenWithStack("normal table(%d) and mark table not match ", modification.TableID))
				}
				err := p.addTable(ctx, tableID, replicaInfo)
				if err != nil {
					return errors.Trace(err)
				}
				opt.Status = model.OperProcessed
				taskStatusChanged = true
			case model.OperProcessed:
				table, exist := p.tables[tableID]
				if !exist {
					log.Panic("table which was added is not found",
						util.ZapFieldChangefeed(ctx), zap.Int64("tableID", tableID))
				}
				localResolvedTs := p.changefeed.taskPosition.ResolvedTs
				globalResolvedTs := p.changefeed.status.ResolvedTs
				if table.ResolvedTs() >= localResolvedTs && localResolvedTs >= globalResolvedTs {
					opt.Status = model.OperFinished
					opt.Done = true
					taskStatusChanged = true
					log.Debug("Operation done signal received",
						util.ZapFieldChangefeed(ctx),
						zap.Int64("tableID", tableID),
						zap.Reflect("operation", opt))
				}
			default:
				log.Panic("unreachable")
			}
		}
	}
	if taskStatusChanged {
		p.changefeed.UpdateTaskStatus(taskStatus)
		log.Info("update table operations",
			util.ZapFieldChangefeed(ctx),
			zap.Reflect("taskStatus", taskStatus))
	}
	return nil
}

func (p *Processor) initTables(ctx context.Context) error {
	for tableID, replicaInfo := range p.changefeed.taskStatus.Tables {
		if _, exist := p.tables[tableID]; !exist {
			err := p.addTable(ctx, tableID, replicaInfo)
			if err != nil {
				return errors.Trace(err)
			}
		}
	}
	return nil
}

func (p *Processor) handlePosition(ctx context.Context) error {
	minResolvedTs := p.ddlPuller.GetResolvedTs()
	for _, table := range p.tables {
		ts := table.ResolvedTs()

		if ts < minResolvedTs {
			minResolvedTs = ts
		}
	}

	phyTs := oracle.ExtractPhysical(minResolvedTs)
	// It is more accurate to get tso from PD, but in most cases we have
	// deployed NTP service, a little bias is acceptable here.
	metricResolvedTsLagGauge.Set(float64(oracle.GetPhysical(time.Now())-phyTs) / 1e3)
	resolvedTsGauge.Set(float64(phyTs))

	// TODO calc checkpointTs
	minCheckpointTs := uint64(0)
	if minResolvedTs > p.changefeed.taskPosition.ResolvedTs ||
		minCheckpointTs > p.changefeed.taskPosition.CheckPointTs {
		p.changefeed.PatchTaskPosition(&model.TaskPosition{
			ResolvedTs:   minResolvedTs,
			CheckPointTs: minCheckpointTs,
			Error:        p.changefeed.taskPosition.Error,
		})
	}
	return nil
}

func (p *Processor) addTable(ctx context.Context, tableID model.TableID, replicaInfo *model.TableReplicaInfo) error {
	if table, ok := p.tables[tableID]; ok {
		if table.Status() == tablepipeline.TableStatusStopping {
			log.Warn("The same table exists but is stopping. Cancel it and continue.", util.ZapFieldChangefeed(ctx), zap.Int64("ID", tableID))
			table.Cancel()
		} else if table.Status() == tablepipeline.TableStatusStopped {
			log.Warn("The same table exists but is stopped. Cancel it and continue.", util.ZapFieldChangefeed(ctx), zap.Int64("ID", tableID))
			table.Cancel()
		} else {
			log.Warn("Ignore existing table", util.ZapFieldChangefeed(ctx), zap.Int64("ID", tableID))
			return nil
		}
	}

	globalCheckpointTs := p.changefeed.status.CheckpointTs
	globalResolvedTs := p.changefeed.status.ResolvedTs

	if replicaInfo.StartTs < globalCheckpointTs {
		log.Warn("addTable: startTs < checkpoint",
			util.ZapFieldChangefeed(ctx),
			zap.Int64("tableID", tableID),
			zap.Uint64("checkpoint", globalCheckpointTs),
			zap.Uint64("startTs", replicaInfo.StartTs))
	}

	cdcCtx := cdccontext.NewContext(ctx, &cdccontext.Vars{
		CaptureAddr:   "TODO: CaptureAddr",
		PDClient:      p.pdCli,
		SchemaStorage: p.schemaStorage,
		Config:        p.changefeed.info.Config,
	})
	kvStorage, err := util.KVStorageFromCtx(ctx)
	if err != nil {
		return errors.Trace(err)
	}
	var tableName string
	err = retry.Run(time.Millisecond*5, 3, func() error {
		if name, ok := p.schemaStorage.GetLastSnapshot().GetTableNameByID(tableID); ok {
			tableName = name.QuoteString()
			return nil
		}
		return errors.Errorf("failed to get table name, fallback to use table id: %d", tableID)
	})
	if err != nil {
		log.Warn("get table name for metric", zap.Error(err))
		tableName = strconv.Itoa(int(tableID))
	}
	resolvedTsGauge := tableResolvedTsGauge.WithLabelValues(p.changefeedID, p.captureInfo.AdvertiseAddr, tableName)

	resolvedTsListener := func(table *tablepipeline.TablePipeline, resolvedTs model.Ts) {
		p.localResolvedNotifier.Notify()
		resolvedTsGauge.Set(float64(oracle.ExtractPhysical(resolvedTs)))
	}

	_, table := tablepipeline.NewTablePipeline(
		cdcCtx,
		p.credential,
		kvStorage,
		p.limitter,
		p.mounter,
		p.changefeed.Engine,
		p.changefeed.SortDir,
		tableID,
		tableName,
		replicaInfo,
		p.changefeed.GetTargetTs(),
		p.outputFromTable,
		resolvedTsListener,
	)

	go func() {
		for _, err := range table.Wait() {
			if cerror.ErrTableProcessorStoppedSafely.Equal(err) || errors.Cause(err) == context.Canceled {
				continue
			}
			p.errCh <- err
		}
	}()

	log.Debug("Add table", zap.Int64("tableID", tableID),
		util.ZapFieldChangefeed(ctx),
		zap.String("name", table.Name()),
		zap.Any("replicaInfo", replicaInfo),
		zap.Uint64("globalResolvedTs", globalResolvedTs))

	p.tables[tableKey] = table

	syncTableNumGauge.WithLabelValues(p.changefeedID, p.captureInfo.AdvertiseAddr).Inc()
}

/*
// globalStatusWorker read global resolve ts from changefeed level info and forward `tableInputChans` regularly.
func (p *processor) globalStatusWorker(ctx context.Context) error {
	log.Info("Global status worker started", util.ZapFieldChangefeed(ctx))

	var (
		changefeedStatus         *model.ChangeFeedStatus
		statusRev                int64
		lastCheckPointTs         uint64
		lastResolvedTs           uint64
		watchKey                 = kv.GetEtcdKeyJob(p.changefeedID)
		globalResolvedTsNotifier = new(notify.Notifier)
	)
	defer globalResolvedTsNotifier.Close()
	globalResolvedTsReceiver, err := globalResolvedTsNotifier.NewReceiver(1 * time.Second)
	if err != nil {
		return err
	}

	updateStatus := func(changefeedStatus *model.ChangeFeedStatus) {
		atomic.StoreUint64(&p.globalcheckpointTs, changefeedStatus.CheckpointTs)
		if lastResolvedTs == changefeedStatus.ResolvedTs &&
			lastCheckPointTs == changefeedStatus.CheckpointTs {
			return
		}
		if lastCheckPointTs < changefeedStatus.CheckpointTs {
			// Delay GC to accommodate pullers starting from a startTs that's too small
			// TODO fix startTs problem and remove GC delay, or use other mechanism that prevents the problem deterministically
			gcTime := oracle.GetTimeFromTS(changefeedStatus.CheckpointTs).Add(-schemaStorageGCLag)
			gcTs := oracle.ComposeTS(gcTime.Unix(), 0)
			p.schemaStorage.DoGC(gcTs)
			lastCheckPointTs = changefeedStatus.CheckpointTs
		}
		if lastResolvedTs < changefeedStatus.ResolvedTs {
			lastResolvedTs = changefeedStatus.ResolvedTs
			atomic.StoreUint64(&p.globalResolvedTs, lastResolvedTs)
			log.Debug("Update globalResolvedTs",
				zap.Uint64("globalResolvedTs", lastResolvedTs), util.ZapFieldChangefeed(ctx))
			globalResolvedTsNotifier.Notify()
		}
	}

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case event := <-p.outputFromTable:
				select {
				case <-ctx.Done():
					return
				case p.output2Sink <- event:
				}
			case <-globalResolvedTsReceiver.C:
				globalResolvedTs := atomic.LoadUint64(&p.globalResolvedTs)
				localResolvedTs := atomic.LoadUint64(&p.localResolvedTs)
				if globalResolvedTs > localResolvedTs {
					log.Warn("globalResolvedTs too large", zap.Uint64("globalResolvedTs", globalResolvedTs),
						zap.Uint64("localResolvedTs", localResolvedTs), util.ZapFieldChangefeed(ctx))
					// we do not issue resolved events if globalResolvedTs > localResolvedTs.
					continue
				}
				select {
				case <-ctx.Done():
					return
				case p.output2Sink <- model.NewResolvedPolymorphicEvent(0, globalResolvedTs):
					// regionID = 0 means the event is produced by TiCDC
				}
			}
		}
	}()

	retryCfg := backoff.WithMaxRetries(
		backoff.WithContext(
			backoff.NewExponentialBackOff(), ctx),
		5,
	)
	for {
		select {
		case <-ctx.Done():
			log.Info("Global resolved worker exited", util.ZapFieldChangefeed(ctx))
			return ctx.Err()
		default:
		}

		err := backoff.Retry(func() error {
			var err error
			changefeedStatus, statusRev, err = p.etcdCli.GetChangeFeedStatus(ctx, p.changefeedID)
			if err != nil {
				if errors.Cause(err) == context.Canceled {
					return backoff.Permanent(err)
				}
				log.Error("Global resolved worker: read global resolved ts failed",
					util.ZapFieldChangefeed(ctx), zap.Error(err))
			}
			return err
		}, retryCfg)
		if err != nil {
			return errors.Trace(err)
		}

		updateStatus(changefeedStatus)

		ch := p.etcdCli.Client.Watch(ctx, watchKey, clientv3.WithRev(statusRev+1), clientv3.WithFilterDelete())
		for resp := range ch {
			if resp.Err() == mvcc.ErrCompacted {
				break
			}
			if resp.Err() != nil {
				return cerror.WrapError(cerror.ErrProcessorEtcdWatch, err)
			}
			for _, ev := range resp.Events {
				var status model.ChangeFeedStatus
				if err := status.Unmarshal(ev.Kv.Value); err != nil {
					return err
				}
				updateStatus(&status)
			}
		}
	}
}
*/
