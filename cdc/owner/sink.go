package owner

import (
	"sync/atomic"

	"github.com/pingcap/errors"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/cdc/sink"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/context"
	"github.com/pingcap/ticdc/pkg/filter"
)

type AsyncSink interface {
	Initialize(ctx context.Context, tableInfo []*model.SimpleTableInfo) error
	EmitCheckpointTs(ctx context.Context, ts uint64) error
	EmitDDLEvent(ctx context.Context, ddl *model.DDLEvent) (bool, error)
	SinkSyncpoint(ctx context.Context, checkpointTs uint64) error
	Close() error
}

type asyncSinkImpl struct {
	sink           sink.Sink
	syncpointStore sink.SyncpointStore

	checkpointTs model.Ts

	checkpointTsCh chan struct{}
	syncpointCh    chan model.Ts
	closeCh        chan struct{}
	errCh          chan error
}

func newAsyncSink(ctx context.Context) (AsyncSink, error) {
	changefeedID := ctx.ChangefeedVars().ID
	changefeedInfo := ctx.ChangefeedVars().Info
	filter, err := filter.NewFilter(changefeedInfo.Config)
	if err != nil {
		return nil, errors.Trace(err)
	}
	errCh := make(chan error, defaultErrChSize)
	s, err := sink.NewSink(ctx, changefeedID, changefeedInfo.SinkURI, filter, changefeedInfo.Config, changefeedInfo.Opts, errCh)
	if err != nil {
		return nil, errors.Trace(err)
	}
	asyncSink := &asyncSinkImpl{
		sink:           s,
		checkpointTsCh: make(chan struct{}, 1024),
		syncpointCh:    make(chan model.Ts, 1024),
		closeCh:        make(chan struct{}, 0),
		errCh:          errCh,
	}
	if changefeedInfo.SyncPointEnabled {
		asyncSink.syncpointStore, err = sink.NewSyncpointStore(ctx, changefeedID, changefeedInfo.SinkURI)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	go asyncSink.run(ctx)
	return asyncSink, nil
}

func (s *asyncSinkImpl) Initialize(ctx context.Context, tableInfo []*model.SimpleTableInfo) error {
	return s.sink.Initialize(ctx, tableInfo)
}

func (s *asyncSinkImpl) run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-s.closeCh:
			return
		case err := <-s.errCh:
			ctx.Throw(err)
		case <-s.checkpointTsCh:
		// todo
		case ts := <-s.syncpointCh:
			// todo
		}
	}
}

func (s *asyncSinkImpl) EmitCheckpointTs(ctx context.Context, ts uint64) error {
	atomic.StoreUint64(&s.checkpointTs, ts)
}

func (s *asyncSinkImpl) EmitDDLEvent(ctx context.Context, ddl *model.DDLEvent) (bool, error) {
	panic("implement me")
}

func (s *asyncSinkImpl) SinkSyncpoint(ctx context.Context, checkpointTs uint64) error {
}

func (s *asyncSinkImpl) Close() (err error) {
	err = s.sink.Close()
	err = s.syncpointStore.Close()
	return
}
