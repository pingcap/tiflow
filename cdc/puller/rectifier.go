package puller

import (
	"context"
	"sync/atomic"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cdc/model"
	"golang.org/x/sync/errgroup"
)

type Rectifier struct {
	EventSorter

	status            model.SorterStatus
	maxSentResolvedTs model.Ts
	targetTs          model.Ts

	outputCh chan *model.PolymorphicEvent
}

func NewRectifier(s EventSorter, targetTs model.Ts) *Rectifier {
	return &Rectifier{
		EventSorter: s,
		targetTs:    targetTs,
		outputCh:    make(chan *model.PolymorphicEvent),
	}
}

func (r *Rectifier) GetStatus() model.SorterStatus {
	return atomic.LoadInt32(&r.status)
}

func (r *Rectifier) GetMaxResolvedTs() model.Ts {
	return atomic.LoadUint64(&r.maxSentResolvedTs)
}

func (r *Rectifier) SafeStop() {
	atomic.CompareAndSwapInt32(&r.status,
		model.SorterStatusWorking,
		model.SorterStatusStopping)
}

func (r *Rectifier) Run(ctx context.Context) error {
	output := func(event *model.PolymorphicEvent) {
		select {
		case <-ctx.Done():
			log.Warn("")
		case r.outputCh <- event:
		}
	}
	errg, ctx := errgroup.WithContext(ctx)
	errg.Go(func() error {
		return r.EventSorter.Run(ctx)
	})
	errg.Go(func() error {
		defer close(r.outputCh)
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case event := <-r.EventSorter.Output():
				if event.CRTs > r.targetTs {
					output(model.NewResolvedPolymorphicEvent(r.targetTs))
					atomic.StoreUint64(&r.maxSentResolvedTs, r.targetTs)
					atomic.StoreInt32(&r.status, model.SorterStatusFinished)
					return nil
				}
				output(event)
				if event.RawKV.OpType == model.OpTypeResolved {
					atomic.StoreUint64(&r.maxSentResolvedTs, event.CRTs)
					switch atomic.LoadInt32(&r.status) {
					case model.SorterStatusStopping:
						atomic.StoreInt32(&r.status, model.SorterStatusStopped)
						return nil
					case model.SorterStatusStopped:
						return nil
					case model.SorterStatusWorking:
					}
				}
			}
		}
	})
	return errg.Wait()
}
func (r *Rectifier) Output() <-chan *model.PolymorphicEvent {
	return r.outputCh
}
