package udflib

import (
	"context"
	"net/url"

	"plugin"

	"github.com/pingcap/tiflow/cdc/contextutil"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
)

type udflibSink struct {
	id model.ChangeFeedID

	addTableFn    tableFunc
	removeTableFn tableFunc
	rowChangedFn  rowChangedFunc
	ddlFn         ddlFunc
}

type tableFunc func(ctx context.Context, tableID int64) error
type rowChangedFunc func(context.Context, ...*model.RowChangedEvent) error
type ddlFunc func(context.Context, *model.DDLEvent) error

func NewUdflibSink(ctx context.Context, sinkURI *url.URL,
	replicaConfig *config.ReplicaConfig, opts map[string]string, errCh chan error,
) (*udflibSink, error) {
	changefeedID := contextutil.ChangefeedIDFromCtx(ctx)

	p := opts["udfpath"]
	plugin, err := plugin.Open(p)
	if err != nil {
		return nil, err
	}

	s := &udflibSink{
		id: changefeedID,
	}

	if addTableFn, err := plugin.Lookup("AddTable"); err == nil {
		s.addTableFn = addTableFn.(func(ctx context.Context, tableID int64) error)
	}
	if removeTableFn, err := plugin.Lookup("RemoveTable"); err == nil {
		s.removeTableFn = removeTableFn.(func(ctx context.Context, tableID int64) error)
	}
	if rowChangedFn, err := plugin.Lookup("RowChanged"); err == nil {
		s.rowChangedFn = rowChangedFn.(func(context.Context, ...*model.RowChangedEvent) error)
	}
	if ddlFn, err := plugin.Lookup("DDL"); err == nil {
		s.ddlFn = ddlFn.(func(context.Context, *model.DDLEvent) error)
	}
	return s, nil
}

func (l *udflibSink) AddTable(tableID model.TableID) error {
	return l.addTableFn(context.Background(), tableID)
}

func (l *udflibSink) EmitRowChangedEvents(ctx context.Context, rows ...*model.RowChangedEvent) error {
	return l.rowChangedFn(ctx, rows...)
}

func (l *udflibSink) EmitDDLEvent(ctx context.Context, ddl *model.DDLEvent) error {
	return l.ddlFn(ctx, ddl)
}

func (l *udflibSink) FlushRowChangedEvents(ctx context.Context, tableID model.TableID, resolved model.ResolvedTs) (model.ResolvedTs, error) {
	// Do nothing now, implement this function when we need it.
	return resolved, nil
}

func (l *udflibSink) EmitCheckpointTs(ctx context.Context, ts uint64, tables []model.TableName) error {
	// Do nothing now, implement this function when we need it.
	return nil
}

func (l *udflibSink) RemoveTable(ctx context.Context, tableID model.TableID) error {
	return l.removeTableFn(ctx, tableID)
}

func (l *udflibSink) Close(ctx context.Context) error {
	return nil
}
