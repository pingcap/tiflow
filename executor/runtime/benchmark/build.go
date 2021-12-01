package benchmark

import (
	"encoding/json"

	"github.com/hanfei1991/microcosm/executor/runtime"
	"github.com/hanfei1991/microcosm/model"
)

func RegisterBuilder() {
	runtime.OpBuilders[model.TableReaderType] = &receiveBuilder{}
	runtime.OpBuilders[model.HashType] = &syncBuilder{}
	runtime.OpBuilders[model.TableSinkType] = &sinkBuilder{}
}

type syncBuilder struct{}

func (r *syncBuilder) Build(op model.Operator) (runtime.Operator, bool, error) {
	return &opSyncer{}, false, nil
}

type receiveBuilder struct{}

func (r *receiveBuilder) Build(op model.Operator) (runtime.Operator, bool, error) {
	cfg := &model.TableReaderOp{}
	err := json.Unmarshal(op, cfg)
	if err != nil {
		return nil, false, err
	}
	return &opReceive{
		flowID: cfg.FlowID,
		addr:   cfg.Addr,
		data:   make(chan *runtime.Record, 1024),
		errCh:  make(chan error, 10),
	}, true, nil
}

type sinkBuilder struct{}

func (r *sinkBuilder) Build(op model.Operator) (runtime.Operator, bool, error) {
	cfg := &model.TableSinkOp{}
	err := json.Unmarshal(op, cfg)
	if err != nil {
		return nil, false, err
	}

	return &opSink{
		writer: fileWriter{
			filePath: cfg.File,
			tid:      cfg.TableID,
		},
	}, false, nil
}
