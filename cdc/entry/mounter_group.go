package entry

import (
	"context"
	"strconv"
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

	changefeedID model.ChangeFeedID
	captureID    model.CaptureID

	index     uint64
	workerNum int
}

const (
	defaultMounterWorkerNum = 16
	defaultOutputChanSize   = 128
	metricsTicker           = 15 * time.Second
)

func NewMounterGroup(
	schemaStorage SchemaStorage,
	workerNum int,
	enableOldValue bool,
	filter filter.Filter,
	tz *time.Location,
	changefeedID model.ChangeFeedID,
	captureID model.CaptureID,
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
		captureID:    captureID,
	}
}

func (m *mounterGroup) Run(ctx context.Context) error {
	defer func() {
		mounterGroupInputChanSizeGauge.DeleteLabelValues(m.changefeedID.Namespace, m.changefeedID.ID)
	}()
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
	mounter := NewMounter(m.schemaStorage, m.changefeedID, m.tz, m.filter, m.enableOldValue)
	rawCh := m.inputCh[index]
	metrics := mounterGroupInputChanSizeGauge.
		WithLabelValues(m.changefeedID.Namespace, m.changefeedID.ID, strconv.Itoa(index))
	ticker := time.NewTicker(15 * time.Second)
	for {
		var pEvent *model.PolymorphicEvent
		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		case <-ticker.C:
			metrics.Set(float64(len(rawCh)))
		case pEvent = <-rawCh:
		}
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

func (m *mounterGroup) AddEvent(ctx context.Context, event *model.PolymorphicEvent) error {
	index := atomic.AddUint64(&m.index, 1) % uint64(m.workerNum)
	select {
	case <-ctx.Done():
		return ctx.Err()
	case m.inputCh[index] <- event:
		return nil
	}
}
