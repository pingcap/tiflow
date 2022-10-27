package entry

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/filter"
	"golang.org/x/sync/errgroup"
)

type MounterGroup interface {
	Run(ctx context.Context) error
	AddEvent(ctx context.Context, event *model.PolymorphicEvent) error
}

type mounterGroup struct {
	schemaStorage  SchemaStorage
	inputCh        []chan *model.PolymorphicEvent
	tz             *time.Location
	filter         filter.Filter
	enableOldValue bool

	workerNum int
	index     uint64

	changefeedID model.ChangeFeedID
}

const (
	defaultMounterWorkerNum = 16
	defaultOutputChanSize   = 16
	defaultMetricInterval   = 15 * time.Second
)

func NewMounterGroup(
	schemaStorage SchemaStorage,
	workerNum int,
	enableOldValue bool,
	filter filter.Filter,
	tz *time.Location,
	changefeedID model.ChangeFeedID,
) MounterGroup {
	if workerNum <= 0 {
		workerNum = defaultMounterWorkerNum
	}
	inputCh := make([]chan *model.PolymorphicEvent, workerNum)
	for i := 0; i < workerNum; i++ {
		inputCh[i] = make(chan *model.PolymorphicEvent, defaultOutputChanSize)
	}
	return &mounterGroup{
		schemaStorage:  schemaStorage,
		inputCh:        inputCh,
		enableOldValue: enableOldValue,
		filter:         filter,
		tz:             tz,

		workerNum: workerNum,

		changefeedID: changefeedID,
	}
}

func (m *mounterGroup) Run(ctx context.Context) error {
	g, ctx := errgroup.WithContext(ctx)
	for i := 0; i < m.workerNum; i++ {
		idx := i
		g.Go(func() error {
			return m.runWorker(ctx, idx)
		})
	}
	return g.Wait()
}

func (m *mounterGroup) runWorker(ctx context.Context, index int) error {
	mounter := NewMounter(m.schemaStorage, m.changefeedID, m.tz, m.enableOldValue)
	rawCh := m.inputCh[index]
	ticker := time.NewTicker(defaultMetricInterval)
	for {
		var pEvent *model.PolymorphicEvent
		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		case <-ticker.C:
		case pEvent = <-rawCh:
			if pEvent.RawKV.OpType == model.OpTypeResolved {
				pEvent.MarkFinished()
				continue
			}
			err := mounter.DecodeEvent(ctx, pEvent)
			if err != nil {
				return errors.Trace(err)
			}
			pEvent.MarkFinished()
		}
	}
}

func (m *mounterGroup) AddEvent(ctx context.Context, event *model.PolymorphicEvent) error {
	index := atomic.AddUint64(&m.index, 1) % uint64(m.workerNum)
	select {
	case <-ctx.Done():
		return ctx.Err()
	case m.inputCh[index] <- event:
		return nil
	}
}
