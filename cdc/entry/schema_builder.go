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

type jobList struct {
	list *list.List
	mu   sync.RWMutex
}

func (l *jobList) FetchNextJobs(currentJob *list.Element, ts uint64) (*list.Element, []*timodel.Job) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	var jobs []*timodel.Job
	if currentJob == nil {
		currentJob = l.list.Front()
	}
	for ; currentJob != nil; currentJob = currentJob.Next() {
		job := currentJob.Value.(*timodel.Job)
		if job.BinlogInfo.FinishedTS > ts {
			break
		}
		jobs = append(jobs, job)
	}
	return currentJob, jobs
}

func (l *jobList) AppendJob(jobs ...*timodel.Job) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, job := range jobs {
		l.list.PushBack(job)
	}
}

func (l *jobList) RemoveOverdueJobs(ts uint64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for e := l.list.Front(); e != nil; e = e.Next() {
		job := e.Value.(*timodel.Job)
		if job.BinlogInfo.FinishedTS >= ts {
			break
		}
		l.list.Remove(e)
	}
}

type StorageBuilder struct {
	baseStorage *Storage
	jobList     *jobList

	resolvedTs uint64
	gcTs       uint64
	ddlEventCh <-chan *model.RawKVEntry
}

func NewStorageBuilder(historyDDL []*timodel.Job, ddlEventCh <-chan *model.RawKVEntry) *StorageBuilder {
	builder := &StorageBuilder{
		jobList: &jobList{
			list: list.New(),
		},
		ddlEventCh: ddlEventCh,
	}

	sort.Slice(historyDDL, func(i, j int) bool {
		return historyDDL[i].BinlogInfo.FinishedTS < historyDDL[j].BinlogInfo.FinishedTS
	})

	if len(historyDDL) > 0 {
		builder.jobList.AppendJob(historyDDL...)
		atomic.StoreUint64(&builder.resolvedTs, historyDDL[len(historyDDL)-1].BinlogInfo.FinishedTS)
	}

	builder.baseStorage = newStorage(&builder.resolvedTs, builder.jobList)
	return builder
}

func (b *StorageBuilder) Run(ctx context.Context) error {
	for {
		var rawKV *model.RawKVEntry
		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		case rawKV = <-b.ddlEventCh:
		}
		if rawKV == nil {
			return errors.Trace(ctx.Err())
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
		b.jobList.AppendJob(job)
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
	b.jobList.RemoveOverdueJobs(ts)
	return nil
}
