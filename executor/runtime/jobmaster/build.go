package jobmaster

import (
	"context"
	"encoding/json"

	"github.com/hanfei1991/microcosm/client"
	"github.com/hanfei1991/microcosm/executor/runtime"
	"github.com/hanfei1991/microcosm/master/jobmaster/benchmark"
	"github.com/hanfei1991/microcosm/master/jobmaster/system"
	"github.com/hanfei1991/microcosm/model"
	"github.com/hanfei1991/microcosm/pkg/errors"
)

func RegisterBuilder() {
	runtime.OpBuilders[model.JobMasterType] = &jobMasterBuilder{}
}

type jobMasterBuilder struct{}

func (b *jobMasterBuilder) Build(op model.Operator) (runtime.Operator, bool, error) {
	cfg := &model.JobMaster{}
	err := json.Unmarshal(op, cfg)
	if err != nil {
		return nil, false, err
	}
	var jobMaster system.JobMaster
	clients := client.NewClientManager()
	err = clients.AddMasterClient(context.Background(), cfg.MasterAddrs)
	if err != nil {
		return nil, false, err
	}
	switch cfg.Tp {
	case model.Benchmark:
		jobMaster, err = benchmark.BuildBenchmarkJobMaster(
			string(cfg.Config), cfg.ID, clients)
	default:
		return nil, false, errors.ErrExecutorUnknownOperator.FastGenByArgs(cfg.Tp)
	}
	if err != nil {
		return nil, false, err
	}
	return &jobMasterAgent{
		master: jobMaster,
	}, true, nil
}
