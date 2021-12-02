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
	runtime.OpBuilders[model.ProducerType] = &producerBuild{}
	runtime.OpBuilders[model.BinlogType] = &binlogBuild{}
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

type producerBuild struct{}

func (b *producerBuild) Build(op model.Operator) (runtime.Operator, bool, error) {
	cfg := &model.ProducerOp{}
	err := json.Unmarshal(op, cfg)
	if err != nil {
		return nil, false, err
	}

	return &opProducer{
		tid:          cfg.TableID,
		dataCnt:      cfg.RecordCnt,
		outputCnt:    cfg.OutputCnt,
		ddlFrequency: cfg.DDLFrequency,
	}, true, nil
}

type binlogBuild struct{}

func (b *binlogBuild) Build(op model.Operator) (runtime.Operator, bool, error) {
	cfg := &model.BinlogOp{}
	err := json.Unmarshal(op, cfg)
	if err != nil {
		return nil, false, err
	}

	return &opBinlog{
		addr: cfg.Address,
	}, false, nil
}
