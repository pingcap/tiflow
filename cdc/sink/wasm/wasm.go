package wasm

import (
	"context"
	"net/url"
	"os"

	"github.com/pingcap/tiflow/cdc/contextutil"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/sink/wasm/wasmcore"
	"github.com/pingcap/tiflow/pkg/config"
)

type wasmPluginSink struct {
	id model.ChangeFeedID

	plugin *wasmcore.WasmPluginWrapper
}

func NewWasmPluginSink(ctx context.Context, sinkURI *url.URL,
	replicaConfig *config.ReplicaConfig, opts map[string]string, errCh chan error,
) (*wasmPluginSink, error) {
	changefeedID := contextutil.ChangefeedIDFromCtx(ctx)

	wasmPath := opts["wasmpath"]
	wasmBytes, err := os.ReadFile(wasmPath)
	if err != nil {
		return nil, err
	}
	plugin, err := wasmcore.NewWasmPlugin(wasmBytes)
	if err != nil {
		return nil, err
	}

	s := &wasmPluginSink{
		id:     changefeedID,
		plugin: plugin,
	}
	return s, nil
}

func (w *wasmPluginSink) AddTable(tableID model.TableID) error {
	ctx := context.Background()

	req := AddTableReq{TableID: tableID}
	return w.doExecWasmCall(ctx, "sink_add_table", req)
}

func (w *wasmPluginSink) EmitRowChangedEvents(ctx context.Context, rows ...*model.RowChangedEvent) error {
	return w.doExecWasmCall(ctx, "sink_emit_row_changed_events", rows)
}

func (w *wasmPluginSink) EmitDDLEvent(ctx context.Context, ddl *model.DDLEvent) error {
	return w.doExecWasmCall(ctx, "sink_emit_ddl_event", ddl)
}

func (w *wasmPluginSink) FlushRowChangedEvents(ctx context.Context, tableID model.TableID, resolved model.ResolvedTs) (model.ResolvedTs, error) {
	// Do nothing now, implement this function when we need it.
	return resolved, nil
}

func (w *wasmPluginSink) EmitCheckpointTs(ctx context.Context, ts uint64, tables []model.TableName) error {
	// Do nothing now, implement this function when we need it.
	return nil
}

func (w *wasmPluginSink) RemoveTable(ctx context.Context, tableID model.TableID) error {
	req := RemoveTableReq{TableID: tableID}
	return w.doExecWasmCall(ctx, "sink_remove_table", req)
}

func (w *wasmPluginSink) Close(ctx context.Context) error {
	return w.plugin.Close(ctx)
}

func (w *wasmPluginSink) doExecWasmCall(ctx context.Context, operation string, req interface{}) error {
	reqBytes, err := WasmMarshal(req)
	if err != nil {
		return err
	}

	respBytes, err := w.plugin.InvokeInstance(ctx, operation, reqBytes)
	if err != nil {
		return err
	}

	var resp CommonExecResp
	if err := WasmUnmarshal(respBytes, &resp); err != nil {
		return err
	}
	if resp.Code != RespCodeSuccess {
		return resp
	}
	return nil
}
