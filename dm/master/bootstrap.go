// Copyright 2020 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package master

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/dm/config"
	"github.com/pingcap/tiflow/dm/master/scheduler"
	"github.com/pingcap/tiflow/dm/master/workerrpc"
	"github.com/pingcap/tiflow/dm/pb"
	"github.com/pingcap/tiflow/dm/pkg/conn"
	tcontext "github.com/pingcap/tiflow/dm/pkg/context"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/dm/pkg/terror"
	"github.com/pingcap/tiflow/dm/pkg/upgrade"
	"github.com/pingcap/tiflow/dm/pkg/v1dbschema"
	"github.com/pingcap/tiflow/dm/pkg/v1workermeta"
	"go.uber.org/zap"
)

var (
	// interval when waiting for the specified count of DM-worker instances become registered when importing from v1.0.x.
	waitWorkerV1Interval = time.Second
	// timeout when waiting for the specified count of DM-worker instances become registered when importing from v1.0.x.
	waitWorkerV1Timeout = 5 * time.Minute
)

// bootstrap bootstraps the cluster, now including:
// - upgrade the cluster from v1.0.x if needed.
// - upgrade the cluster from a previous v2.0.x version to the current version.
func (s *Server) bootstrap(ctx context.Context) error {
	log.L().Info("start bootstrapping")
	if s.cfg.V1SourcesPath != "" {
		err := s.importFromV10x(ctx)
		if err != nil {
			return terror.ErrMasterFailToImportFromV10x.Delegate(err)
		}
	} else {
		uctx := upgrade.Context{
			Context:        ctx,
			SubTaskConfigs: s.scheduler.GetSubTaskCfgs(),
		}
		err := upgrade.TryUpgrade(s.etcdClient, uctx)
		if err != nil {
			return err
		}
	}
	log.L().Info("successful bootstrapped")
	return nil
}

func (s *Server) bootstrapBeforeSchedulerStart(ctx context.Context) error {
	log.L().Info("bootstrap before scheduler start")
	// no need for v1.0.x
	if s.cfg.V1SourcesPath != "" {
		return nil
	}

	return upgrade.TryUpgradeBeforeSchedulerStart(ctx, s.etcdClient)
}

// importFromV10x tries to import/upgrade the cluster from v1.0.x.
func (s *Server) importFromV10x(ctx context.Context) error {
	// 1. check whether need to upgrade based on the cluster version.
	preVer, _, err := upgrade.GetVersion(s.etcdClient)
	if err != nil {
		return err
	} else if !preVer.NotSet() {
		return nil // v2.0.x cluster, no need to import.
	}

	logger := log.L().WithFields(zap.String("op", "import from v1.0.x"))
	tctx := tcontext.NewContext(ctx, logger)

	// 2. collect source config files.
	logger.Info("collecting source config files", zap.String("path", s.cfg.V1SourcesPath))
	sourceCfgs, err := s.collectSourceConfigFilesV1Import(tctx)
	if err != nil {
		return err
	}

	// 3. wait for all DM-worker instances ready.
	logger.Info("waiting for all DM-worker instances ready", zap.Int("count", len(sourceCfgs)))
	err = s.waitWorkersReadyV1Import(tctx, sourceCfgs)
	if err != nil {
		return err
	}

	// 4. get subtasks config and stage from DM-worker instances.
	logger.Info("getting subtask config and status from DM-worker")
	subtaskCfgs, subtaskStages, err := s.getSubtaskCfgsStagesV1Import(tctx)
	if err != nil {
		return err
	}

	// 5. upgrade v1.0.x downstream metadata table and run v2.0 upgrading routines.
	//    some v2.0 upgrading routines are also altering schema, if we run them after adding sources, DM worker will
	//    meet error.
	logger.Info("upgrading downstream metadata tables")
	err = s.upgradeDBSchemaV1Import(tctx, subtaskCfgs)
	if err != nil {
		return err
	}
	uctx := upgrade.Context{
		Context:        ctx,
		SubTaskConfigs: subtaskCfgs,
	}
	err = upgrade.UntouchVersionUpgrade(s.etcdClient, uctx)
	if err != nil {
		return err
	}

	// 6. create sources.
	logger.Info("add source config into cluster")
	err = s.addSourcesV1Import(tctx, sourceCfgs)
	if err != nil {
		return err
	}

	// 7. create subtasks with the specified stage.
	logger.Info("creating subtasks")
	err = s.createSubtaskV1Import(tctx, subtaskCfgs, subtaskStages)
	if err != nil {
		return err
	}

	// 8. mark the upgrade operation as done.
	logger.Info("marking upgrade from v1.0.x as done")
	_, err = upgrade.PutVersion(s.etcdClient, upgrade.CurrentVersion)
	if err != nil {
		return err
	}

	// 9. clear v1.0.x data (source config files, DM-worker metadata), failed is not a problem.
	logger.Info("clearing v1.0.x data")
	s.clearOldDataV1Import(tctx)

	// NOTE: add any other mechanisms to report the `done` of processing if needed later.
	logger.Info("importing from v1.0.x has done")
	return nil
}

// collectSourceConfigFilesV1Import tries to collect source config files for v1.0.x importing.
func (s *Server) collectSourceConfigFilesV1Import(tctx *tcontext.Context) (map[string]*config.SourceConfig, error) {
	files, err := os.ReadDir(s.cfg.V1SourcesPath)
	if err != nil {
		return nil, err
	}

	cfgs := make(map[string]*config.SourceConfig)
	for _, f := range files {
		if f.IsDir() {
			continue // ignore sub directories.
		}

		fp := filepath.Join(s.cfg.V1SourcesPath, f.Name())
		content, err := os.ReadFile(fp)
		if err != nil {
			return nil, err
		}

		cfgs2, err := parseAndAdjustSourceConfig(tctx.Ctx, []string{string(content)})
		if err != nil {
			// abort importing if any invalid source config files exist.
			return nil, err
		}

		cfgs[cfgs2[0].SourceID] = cfgs2[0]
		tctx.Logger.Info("collected source config", zap.Stringer("config", cfgs2[0]))
	}

	return cfgs, nil
}

// waitWorkersReadyV1Import waits for DM-worker instances ready for v1.0.x importing.
// NOTE: in v1.0.x, `count of DM-worker instances` equals `count of source config files`.
func (s *Server) waitWorkersReadyV1Import(tctx *tcontext.Context, sourceCfgs map[string]*config.SourceConfig) error {
	// now, we simply check count repeatedly.
	count := len(sourceCfgs)
	ctx2, cancel2 := context.WithTimeout(context.Background(), waitWorkerV1Timeout)
	defer cancel2()
	for {
		select {
		case <-tctx.Ctx.Done():
			return tctx.Ctx.Err()
		case <-ctx2.Done():
			return errors.Errorf("wait for DM-worker instances timeout in %v", waitWorkerV1Timeout)
		case <-time.After(waitWorkerV1Interval):
			workers, err := s.scheduler.GetAllWorkers()
			if err != nil {
				return err
			}
			if len(workers) >= count {
				tctx.Logger.Info("all DM-worker instances ready", zap.Int("ready count", len(workers)))
				return nil
			}
			tctx.Logger.Info("waiting for DM-worker instances ready", zap.Int("ready count", len(workers)))
		}
	}
}

// getSubtaskCfgsStagesV1Import tries to get all subtask config and stage from DM-worker instances.
// returned:
//   - configs: task-name -> source-ID -> subtask config.
//   - stages: task-name -> source-ID -> subtask stage.
func (s *Server) getSubtaskCfgsStagesV1Import(tctx *tcontext.Context) (
	map[string]map[string]config.SubTaskConfig, map[string]map[string]pb.Stage, error,
) {
	workers, err := s.scheduler.GetAllWorkers()
	if err != nil {
		return nil, nil, err
	}

	req := workerrpc.Request{
		Type:          workerrpc.CmdOperateV1Meta,
		OperateV1Meta: &pb.OperateV1MetaRequest{Op: pb.V1MetaOp_GetV1Meta},
	}
	respCh := make(chan *pb.OperateV1MetaResponse, len(workers))
	var wg sync.WaitGroup
	for _, worker := range workers {
		wg.Add(1)
		go func(worker *scheduler.Worker) {
			defer wg.Done()
			resp, err := worker.SendRequest(tctx.Ctx, &req, s.cfg.RPCTimeout)
			if err != nil {
				respCh <- &pb.OperateV1MetaResponse{
					Result: false,
					Msg:    fmt.Sprintf("worker %s, %s", worker.BaseInfo().String(), err.Error()),
				}
			} else {
				respCh <- resp.OperateV1Meta
			}
		}(worker)
	}
	wg.Wait()

	subtaskCfgs := make(map[string]map[string]config.SubTaskConfig)
	subtaskStages := make(map[string]map[string]pb.Stage)
	errorMsgs := make([]string, 0)
	for len(respCh) > 0 {
		resp := <-respCh
		if !resp.Result {
			errorMsgs = append(errorMsgs, resp.Msg)
			continue
		}

		for taskName, meta := range resp.Meta {
			cfg, err := v1workermeta.SubTaskConfigFromV1TOML(meta.Task)
			if err != nil {
				tctx.Logger.Error("fail to get subtask config from v1 TOML", zap.ByteString("config", meta.Task))
				errorMsgs = append(errorMsgs, fmt.Sprintf("task %s, %s", meta.Name, err.Error()))
				continue
			}

			if _, ok := subtaskCfgs[taskName]; !ok {
				subtaskCfgs[taskName] = make(map[string]config.SubTaskConfig)
				subtaskStages[taskName] = make(map[string]pb.Stage)
			}
			subtaskCfgs[taskName][cfg.SourceID] = cfg
			subtaskStages[taskName][cfg.SourceID] = meta.Stage
			tctx.Logger.Info("got subtask config and stage", zap.Stringer("config", &cfg), zap.Stringer("stage", meta.Stage))
		}
	}

	if len(errorMsgs) > 0 {
		// if failed for any DM-worker instances, we abort the importing process now.
		return nil, nil, errors.Errorf("fail to get subtask config and stage: %s", strings.Join(errorMsgs, ","))
	}

	return subtaskCfgs, subtaskStages, nil
}

// addSourcesV1Import tries to add source config into the cluster for v1.0.x importing.
func (s *Server) addSourcesV1Import(tctx *tcontext.Context, cfgs map[string]*config.SourceConfig) error {
	var (
		added []string
		err   error
	)
	for _, cfg := range cfgs {
		err = s.scheduler.AddSourceCfg(cfg)
		if err != nil {
			if terror.ErrSchedulerSourceCfgExist.Equal(err) {
				err = nil // reset error
				tctx.Logger.Warn("source already exists", zap.String("source", cfg.SourceID))
			} else {
				break
			}
		} else {
			added = append(added, cfg.SourceID)
		}
	}

	if err != nil {
		// try to remove source configs if any error occurred, but it's not a big problem if failed.
		for _, sid := range added {
			err2 := s.scheduler.RemoveSourceCfg(sid)
			if err2 != nil {
				tctx.Logger.Error("fail to remove source config", zap.String("source", sid), zap.Error(err2))
			}
		}
	}
	return err
}

// upgradeDBSchemaV1Import tries to upgrade the metadata DB schema (and data) for v1.0.x importing.
func (s *Server) upgradeDBSchemaV1Import(tctx *tcontext.Context, cfgs map[string]map[string]config.SubTaskConfig) error {
	for _, taskCfgs := range cfgs {
		// all subtasks in one task must have the same downstream, so we only create one BaseDB instance.
		// but different tasks may have different downstream.
		var targetDB *conn.BaseDB
		for _, cfg := range taskCfgs {
			cfg2, err := cfg.DecryptedClone() // `cfg` should already be `Adjust`.
			if err != nil {
				return err
			}
			if targetDB == nil {
				targetDB, err = conn.GetDownstreamDB(&cfg2.To)
				if err != nil {
					return err
				}
			}
			err = v1dbschema.UpdateSchema(tctx, targetDB, cfg2)
			if err != nil {
				targetDB.Close()
				return err
			}
		}
		targetDB.Close() // close BaseDB for this task.
	}
	return nil
}

// createSubtaskV1Import tries to create subtasks with the specified stage.
// NOTE: we do not stopping previous subtasks if any later one failed (because some side effects may have taken),
// and let the user to check & fix the problem.
// TODO(csuzhangxc): merge subtask configs to support `get-task-config`.
func (s *Server) createSubtaskV1Import(tctx *tcontext.Context,
	cfgs map[string]map[string]config.SubTaskConfig, stages map[string]map[string]pb.Stage,
) error {
	var err error
outerLoop:
	for taskName, taskCfgs := range cfgs {
		for sourceID, cfg := range taskCfgs {
			var cfg2 *config.SubTaskConfig
			cfg2, err = cfg.DecryptedClone()
			if err != nil {
				break outerLoop
			}
			stage := stages[taskName][sourceID]
			switch stage {
			case pb.Stage_Running, pb.Stage_Paused:
			default:
				tctx.Logger.Warn("skip to create subtask because only support to create subtasks with Running/Paused stage now", zap.Stringer("stage", stage))
				continue
			}
			err = s.scheduler.AddSubTasks(false, stage, *cfg2)
			if err != nil {
				if terror.ErrSchedulerSubTaskExist.Equal(err) {
					err = nil // reset error
					tctx.Logger.Warn("subtask already exists", zap.String("task", taskName), zap.String("source", sourceID))
				} else {
					break outerLoop
				}
			}
		}
	}
	return err
}

// clearOldDataV1Import tries to clear v1.0.x data after imported, now these data including:
// - source config files.
// - DM-worker metadata.
func (s *Server) clearOldDataV1Import(tctx *tcontext.Context) {
	tctx.Logger.Info("removing source config files", zap.String("path", s.cfg.V1SourcesPath))
	err := os.RemoveAll(s.cfg.V1SourcesPath)
	if err != nil {
		tctx.Logger.Error("fail to remove source config files", zap.String("path", s.cfg.V1SourcesPath))
	}

	workers, err := s.scheduler.GetAllWorkers()
	if err != nil {
		tctx.Logger.Error("fail to get DM-worker agents")
		return
	}

	req := workerrpc.Request{
		Type:          workerrpc.CmdOperateV1Meta,
		OperateV1Meta: &pb.OperateV1MetaRequest{Op: pb.V1MetaOp_RemoveV1Meta},
	}
	var wg sync.WaitGroup
	for _, worker := range workers {
		wg.Add(1)
		go func(worker *scheduler.Worker) {
			defer wg.Done()
			tctx.Logger.Info("removing DM-worker metadata", zap.Stringer("worker", worker.BaseInfo()))
			_, err2 := worker.SendRequest(tctx.Ctx, &req, s.cfg.RPCTimeout)
			if err2 != nil {
				tctx.Logger.Error("fail to remove metadata for DM-worker", zap.Stringer("worker", worker.BaseInfo()))
			}
		}(worker)
	}
	wg.Wait()
}
