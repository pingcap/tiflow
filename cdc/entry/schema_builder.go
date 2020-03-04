package entry

import (
	"container/list"
	"context"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/cenkalti/backoff"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	timodel "github.com/pingcap/parser/model"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/pkg/retry"
	"go.uber.org/zap"
)

type jobList struct {
	list *list.List
	mu   sync.RWMutex
	gcTs uint64
}

func newJobList() *jobList {
	j := &jobList{
		list: list.New(),
	}
	j.list.PushBack((*timodel.Job)(nil))
	return j
}

func (l *jobList) FetchNextJobs(currentJob *list.Element, ts uint64) (*list.Element, []*timodel.Job) {
	if currentJob == nil {
		log.Fatal("param `currentJob` in `FetchNextJobs` can't be nil, please report a bug")
	}

	l.mu.RLock()
	defer l.mu.RUnlock()

	if ts < l.gcTs {
		log.Fatal("cannot fetch the jobs which of finishedTs is less then gcTs, please report a bug", zap.Uint64("gcTs", l.gcTs))
	}

	if currentJob != l.list.Front() {
		job := currentJob.Value.(*timodel.Job)
		if job.BinlogInfo.FinishedTS <= l.gcTs {
			currentJob = l.list.Front()
		}
	}
	var jobs []*timodel.Job

	for nextJob := currentJob.Next(); nextJob != nil; nextJob = nextJob.Next() {
		job := nextJob.Value.(*timodel.Job)
		if job.BinlogInfo.FinishedTS > ts {
			break
		}
		jobs = append(jobs, job)
		currentJob = nextJob
	}
	return currentJob, jobs
}

func (l *jobList) AppendJob(jobs ...*timodel.Job) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, job := range jobs {
		if job.BinlogInfo.FinishedTS < l.gcTs {
			log.Fatal("cannot append a job which of finishedTs is less then gcTs, please report a bug", zap.Uint64("gcTs", l.gcTs), zap.Reflect("job", job))
		}
		l.list.PushBack(job)
	}
}

func (l *jobList) RemoveOverdueJobs(ts uint64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for e := l.list.Front().Next(); e != nil; {
		job := e.Value.(*timodel.Job)
		if job.BinlogInfo.FinishedTS > ts {
			break
		}
		l.gcTs = job.BinlogInfo.FinishedTS
		lastE := e
		e = e.Next()
		l.list.Remove(lastE)
	}
}

func (l *jobList) Head() *list.Element {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.list.Front()
}

// StorageBuilder creates and manages many schema storage
type StorageBuilder struct {
	baseStorage   *Storage
	baseStorageMu sync.Mutex
	jobList       *jobList

	resolvedTs uint64
	gcTs       uint64
	ddlEventCh <-chan *model.RawKVEntry
}

// NewStorageBuilder creates a new StorageBuilder
func NewStorageBuilder(historyDDL []*timodel.Job, ddlEventCh <-chan *model.RawKVEntry) *StorageBuilder {
	builder := &StorageBuilder{
		jobList:    newJobList(),
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

// Run runs the StorageBuilder
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
		if rawKV.Ts < b.resolvedTs {
			continue
		}

		if rawKV.OpType == model.OpTypeResolved {
			atomic.StoreUint64(&b.resolvedTs, rawKV.Ts)
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
		atomic.StoreUint64(&b.resolvedTs, rawKV.Ts)
	}
}

// Build creates a new schema storage,
// every storage craeted by one builder is associated with each other,
// they share the resolvedTs and job list
func (b *StorageBuilder) Build(ts uint64) (*Storage, error) {
	if ts < b.gcTs {
		log.Fatal("the parameter `ts` in function `StorageBuilder.Build` should never less than gcTs, please report a bug.")
	}

	b.baseStorageMu.Lock()
	c := b.baseStorage.Clone()
	b.baseStorageMu.Unlock()

	err := retry.Run(func() error {
		err := c.HandlePreviousDDLJobIfNeed(ts)
		if errors.Cause(err) != model.ErrUnresolved {
			return backoff.Permanent(err)
		}
		return err
	}, 5)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return c, nil
}

// GetResolvedTs return the resolvedTs of DDL puller in this StorageBuilder
func (b *StorageBuilder) GetResolvedTs() uint64 {
	return atomic.LoadUint64(&b.resolvedTs)
}

// DoGc removes the jobs which of finishedTs is less then gcTs
func (b *StorageBuilder) DoGc(ts uint64) error {
	if ts > atomic.LoadUint64(&b.resolvedTs) {
		log.Fatal("gcTs is greater than resolvedTs in StorageBuilder, please report a bug", zap.Uint64("gcTs", ts))
	}
	b.baseStorageMu.Lock()
	defer b.baseStorageMu.Unlock()
	err := b.baseStorage.HandlePreviousDDLJobIfNeed(ts)
	if err != nil {
		return errors.Trace(err)
	}
	b.jobList.RemoveOverdueJobs(ts)
	return nil
}
