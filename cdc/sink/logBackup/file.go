package logBackup

import (
	"context"

	"github.com/pingcap/ticdc/cdc/model"
)

type fileSink struct {
}

func (f *fileSink) EmitRowChangedEvents(ctx context.Context, rows ...*model.RowChangedEvent) error {
	return nil
}

func (f *fileSink) FlushRowChangedEvents(ctx context.Context, resolvedTs uint64) error {
	return nil
}

func (f *fileSink) EmitCheckpointTs(ctx context.Context, ts uint64) error {
	return nil
}

func (f *fileSink) EmitDDLEvent(ctx context.Context, ddl *model.DDLEvent) error {
	return nil
}

func (f *fileSink) Initialize(ctx context.Context, tableInfo []*model.TableInfo) error {
	return nil
}

func (f *fileSink) Close() error {
	return nil
}

func (f *fileSink) run(ctx context.Context) error {
	return nil
}

func NewLocalFileSink() *fileSink {
	return &fileSink{}
}
