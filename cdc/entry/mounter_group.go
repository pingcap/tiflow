package entry

import (
	"context"
	"math/rand"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/filter"
	"golang.org/x/sync/errgroup"
)

type MounterGroup interface {
	Run(ctx context.Context) error
	Input() chan<- *model.PolymorphicEvent
}

type mounterGroup struct {
	schemaStorage  SchemaStorage
	rawCh          []chan *model.PolymorphicEvent
	tz             *time.Location
	filter         filter.Filter
	enableOldValue bool

	changefeedID model.ChangeFeedID
	captureID    model.CaptureID

	workerNum int
}

const (
	defaultMounterWorkerNum = 32
	defaultOutputChanSize   = 128000
)

func NewMounterGroup(
	schemaStorage SchemaStorage,
	workerNum int,
	enableOldValue bool,
	filter filter.Filter,
	changefeedID model.ChangeFeedID,
	captureID model.CaptureID,
) MounterGroup {
	if workerNum <= 0 {
		workerNum = defaultMounterWorkerNum
	}
	chs := make([]chan *model.PolymorphicEvent, workerNum)
	for i := 0; i < workerNum; i++ {
		chs[i] = make(chan *model.PolymorphicEvent, defaultOutputChanSize)
	}
	return &mounterGroup{
		schemaStorage:  schemaStorage,
		rawCh:          chs,
		enableOldValue: enableOldValue,
		filter:         filter,
		workerNum:      workerNum,

		changefeedID: changefeedID,
		captureID:    captureID,
	}
}

func (m *mounterGroup) Run(ctx context.Context) error {
	g, ctx := errgroup.WithContext(ctx)
	for i := 0; i < m.workerNum; i++ {
		g.Go(func() error {
			return m.runWorker(ctx, i)
		})
	}
	return g.Wait()
}

func (m *mounterGroup) runWorker(ctx context.Context, index int) error {
	mounter := NewMounter(m.schemaStorage, m.changefeedID, m.tz, m.filter, m.enableOldValue)
	for {
		var pEvent *model.PolymorphicEvent
		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		case pEvent = <-m.rawCh[index]:
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

func (m *mounterGroup) Input() chan<- *model.PolymorphicEvent {
	return m.rawCh[rand.Intn(m.workerNum)]
}
