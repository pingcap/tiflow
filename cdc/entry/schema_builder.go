package entry

import (
	"container/list"
	"context"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/pingcap/log"
	"go.uber.org/zap"

	"github.com/pingcap/errors"
	timodel "github.com/pingcap/parser/model"
	"github.com/pingcap/ticdc/cdc/model"
)

type StorageBuilder struct {
	baseStorage *Storage

	jobList *struct {
		*list.List
		*sync.RWMutex
	}

	resolvedTs uint64
	gcTs       uint64
	ddlEventCh <-chan *model.RawKVEntry
}

func NewStorageBuilder(historyDDL []*timodel.Job, ddlEventCh <-chan *model.RawKVEntry) (*StorageBuilder, error) {
	builder := &StorageBuilder{
		jobList: &struct {
			*list.List
			*sync.RWMutex
		}{List: list.New(), RWMutex: new(sync.RWMutex)},
		ddlEventCh: ddlEventCh,
	}

	// push a head element to list
	builder.jobList.PushBack(&timodel.Job{})

	sort.Slice(historyDDL, func(i, j int) bool {
		return historyDDL[i].BinlogInfo.FinishedTS < historyDDL[j].BinlogInfo.FinishedTS
	})

	for _, job := range historyDDL {
		builder.jobList.PushBack(job)
		atomic.StoreUint64(&builder.resolvedTs, job.BinlogInfo.FinishedTS)
	}

	builder.baseStorage = newStorage(&builder.resolvedTs, builder.jobList.Front(), builder.jobList.RWMutex)
	return builder, nil
}

func (b *StorageBuilder) Run(ctx context.Context) error {
	for {
		var rawKV *model.RawKVEntry
		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		case rawKV = <-b.ddlEventCh:
		}
		if rawKV.Ts <= b.resolvedTs {
			continue
		}

		atomic.StoreUint64(&b.resolvedTs, rawKV.Ts)
		log.Info("DDL resolvedts", zap.Uint64("ts", rawKV.Ts))
		if rawKV.OpType == model.OpTypeResolved {
			continue
		}

		job, err := UnmarshalDDL(rawKV)
		if err != nil {
			return errors.Trace(err)
		}
		if job == nil {
			continue
		}
		b.jobList.Lock()
		b.jobList.PushBack(job)
		b.jobList.Unlock()
	}
}

func (b *StorageBuilder) Build(ts uint64) *Storage {
	if ts < b.gcTs {
		log.Fatal("the parameter `ts` in function `StorageBuilder.Build` should never less than gcTs, please report a bug.")
	}
	return b.baseStorage.Clone()
}

func (b *StorageBuilder) DoGc(ts uint64) error {
	resolvedTs := atomic.LoadUint64(&b.resolvedTs)
	if ts > resolvedTs {
		ts = resolvedTs
	}
	err := b.baseStorage.HandlePreviousDDLJobIfNeed(ts)
	if err != nil {
		return errors.Trace(err)
	}
	b.jobList.Lock()
	defer b.jobList.Unlock()
	for e := b.jobList.Front().Next(); e != nil; e = e.Next() {
		job := e.Value.(*timodel.Job)
		if job.BinlogInfo.FinishedTS > ts {
			break
		}
		b.jobList.Remove(e)
	}
	return nil
}
