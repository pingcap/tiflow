package owner

import (
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/cdc/sink"
	"github.com/pingcap/ticdc/pkg/context"
	"github.com/pingcap/ticdc/pkg/filter"
)

type AsyncSink interface {
	Initialize(ctx context.Context, tableInfo []*model.SimpleTableInfo) error
	EmitCheckpointTs(ctx context.Context, ts uint64)
	EmitDDLEvent(ctx context.Context, ddl *model.DDLEvent) (bool, error)
	SinkSyncpoint(ctx context.Context, checkpointTs uint64) error
	Close() error
}

type asyncSinkImpl struct {
	sink           sink.Sink
	syncpointStore sink.SyncpointStore

	checkpointTs model.Ts

	ddlCh         chan *model.DDLEvent
	ddlFinishedTs model.Ts
	ddlSentTs     model.Ts

	closeCh chan struct{}
	errCh   chan error
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
		sink:    s,
		ddlCh:   make(chan *model.DDLEvent, 1),
		closeCh: make(chan struct{}, 0),
		errCh:   errCh,
	}
	if changefeedInfo.SyncPointEnabled {
		asyncSink.syncpointStore, err = sink.NewSyncpointStore(ctx, changefeedID, changefeedInfo.SinkURI)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if err := asyncSink.syncpointStore.CreateSynctable(ctx); err != nil {
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
	// TODO make the tick duration configurable
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	var lastCheckpointTs model.Ts
	for {
		select {
		case <-ctx.Done():
			return
		case <-s.closeCh:
			return
		case err := <-s.errCh:
			ctx.Throw(err)
			return
		case <-ticker.C:
			checkpointTs := atomic.LoadUint64(&s.checkpointTs)
			if checkpointTs <= lastCheckpointTs {
				continue
			}
			lastCheckpointTs = checkpointTs
			if err := s.sink.EmitCheckpointTs(ctx, checkpointTs); err != nil {
				ctx.Throw(errors.Trace(err))
				return
			}
		case ddl := <-s.ddlCh:
			if err := s.sink.EmitDDLEvent(ctx, ddl); err != nil {
				ctx.Throw(errors.Trace(err))
				return
			}
			atomic.StoreUint64(&s.ddlFinishedTs, ddl.CommitTs)
		}
	}
}

func (s *asyncSinkImpl) EmitCheckpointTs(ctx context.Context, ts uint64) {
	atomic.StoreUint64(&s.checkpointTs, ts)
}

func (s *asyncSinkImpl) EmitDDLEvent(ctx context.Context, ddl *model.DDLEvent) (bool, error) {
	ddlFinishedTs := atomic.LoadUint64(&s.ddlFinishedTs)
	if ddl.CommitTs <= ddlFinishedTs {
		return true, nil
	}
	if ddl.CommitTs <= s.ddlSentTs {
		return false, nil
	}
	select {
	case <-ctx.Done():
		return false, errors.Trace(ctx.Err())
	case s.ddlCh <- ddl:
	}
	s.ddlSentTs = ddl.CommitTs
	return false, nil
}

func (s *asyncSinkImpl) SinkSyncpoint(ctx context.Context, checkpointTs uint64) error {
	// TODO implement async sink syncpoint
	return s.syncpointStore.SinkSyncpoint(ctx, ctx.ChangefeedVars().ID, checkpointTs)
}

func (s *asyncSinkImpl) Close() (err error) {
	err = s.sink.Close()
	err = s.syncpointStore.Close()
	return
}
