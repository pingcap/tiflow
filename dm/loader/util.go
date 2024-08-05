// Copyright 2019 PingCAP, Inc.
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

package loader

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tiflow/dm/config"
	"github.com/pingcap/tiflow/dm/pkg/dumpling"
	"github.com/pingcap/tiflow/dm/pkg/ha"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/dm/pkg/storage"
	"github.com/pingcap/tiflow/dm/pkg/terror"
	"github.com/pingcap/tiflow/dm/pkg/utils"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

// percent calculates percentage of a/b.
func percent(a int64, b int64, finish bool) string {
	if b == 0 {
		if finish {
			return "100.00 %"
		}
		return "0.00 %"
	}
	return fmt.Sprintf("%.2f %%", float64(a)/float64(b)*100)
}

func getMydumpMetadata(ctx context.Context, cli *clientv3.Client, cfg *config.SubTaskConfig, workerName string) (string, string, error) {
	metafile := "metadata"
	failpoint.Inject("TestRemoveMetaFile", func() {
		err := storage.RemoveAll(ctx, cfg.LoaderConfig.Dir, nil)
		if err != nil {
			log.L().Warn("TestRemoveMetaFile Error", log.ShortError(err))
		}
	})
	loc, _, err := dumpling.ParseMetaData(ctx, cfg.LoaderConfig.Dir, metafile, cfg.ExtStorage)
	if err == nil {
		return loc.Position.String(), loc.GTIDSetStr(), nil
	}
	if storage.IsNotExistError(err) {
		failpoint.Inject("TestRemoveMetaFile", func() {
			panic("success check file not exist!!")
		})
		worker, err2 := getLoadTask(cli, cfg.Name, cfg.SourceID)
		if err2 != nil {
			log.L().Warn("get load task", log.ShortError(err2))
		}
		if worker != "" && worker != workerName {
			return "", "", terror.ErrLoadTaskWorkerNotMatch.Generate(worker, workerName)
		}
		return "", "", terror.ErrParseMydumperMeta.Generate(err, "not found")
	}
	if terror.ErrMetadataNoBinlogLoc.Equal(err) {
		log.L().Warn("dumped metadata doesn't have binlog location, it's OK if DM doesn't enter incremental mode")
		return "", "", nil
	}

	toPrint, err2 := storage.ReadFile(ctx, cfg.Dir, metafile, nil)
	if err2 != nil {
		toPrint = []byte(err2.Error())
	}
	log.L().Error("fail to parse dump metadata", log.ShortError(err))
	return "", "", terror.ErrParseMydumperMeta.Generate(err, toPrint)
}

// cleanDumpFiles is called when finish restoring data, to clean useless files.
func cleanDumpFiles(ctx context.Context, cfg *config.SubTaskConfig) {
	log.L().Info("clean dump files")
	if cfg.Mode == config.ModeFull {
		// in full-mode all files won't be need in the future
		if err := storage.RemoveAll(ctx, cfg.Dir, nil); err != nil {
			log.L().Warn("error when remove loaded dump folder", zap.String("data folder", cfg.Dir), zap.Error(err))
		}
	} else {
		if storage.IsS3Path(cfg.Dir) {
			// s3 no need immediately remove
			log.L().Info("dump path is s3, and s3 storage does not need to immediately remove dump data files.", zap.String("S3 Path", cfg.Dir))
			return
		}
		// leave metadata file and table structure files, only delete data files
		files, err := utils.CollectDirFiles(cfg.Dir)
		if err != nil {
			log.L().Warn("fail to collect files", zap.String("data folder", cfg.Dir), zap.Error(err))
		}
		var lastErr error
		for f := range files {
			if strings.HasSuffix(f, ".sql") {
				if strings.HasSuffix(f, "-schema-create.sql") || strings.HasSuffix(f, "-schema.sql") {
					continue
				}
				lastErr = os.Remove(filepath.Join(cfg.Dir, f))
			}
		}
		if lastErr != nil {
			log.L().Warn("show last error when remove loaded dump sql files", zap.String("data folder", cfg.Dir), zap.Error(lastErr))
		}
	}
}

// putLoadTask is called when start restoring data, to put load worker in etcd.
// This is no-op when the `cli` argument is nil.
func putLoadTask(cli *clientv3.Client, cfg *config.SubTaskConfig, workerName string) error {
	// some usage like DM as a library, we don't support this feature
	if cli == nil {
		return nil
	}
	_, err := ha.PutLoadTask(cli, cfg.Name, cfg.SourceID, workerName)
	if err != nil {
		return err
	}
	log.L().Info("put load worker in etcd", zap.String("task", cfg.Name), zap.String("source", cfg.SourceID), zap.String("worker", workerName))
	return nil
}

// delLoadTask is called when finish restoring data, to delete load worker in etcd.
// This is no-op when the `cli` argument is nil.
func delLoadTask(cli *clientv3.Client, cfg *config.SubTaskConfig, workerName string) error {
	// some usage like DM as a library, we don't support this feature
	if cli == nil {
		return nil
	}
	_, _, err := ha.DelLoadTask(cli, cfg.Name, cfg.SourceID)
	if err != nil {
		return err
	}
	log.L().Info("delete load worker in etcd for full mode", zap.String("task", cfg.Name), zap.String("source", cfg.SourceID), zap.String("worker", workerName))
	return nil
}

// getLoadTask gets the worker which in load stage for the source of the subtask.
// It will return "" and no error when the `cli` argument is nil.
func getLoadTask(cli *clientv3.Client, task, sourceID string) (string, error) {
	if cli == nil {
		return "", nil
	}
	name, _, err := ha.GetLoadTask(cli, task, sourceID)
	return name, err
}

// readyAndWait updates the lightning status of this worker to LightningReady and
// waits for all workers' status not LightningNotReady.
// Only works for physical import.
func readyAndWait(ctx context.Context, cli *clientv3.Client, cfg *config.SubTaskConfig) error {
	return putAndWait(ctx, cli, cfg, ha.LightningReady, func(s string) bool {
		return s == ha.LightningNotReady
	})
}

// finishAndWait updates the lightning status of this worker to LightningFinished
// and waits for all workers' status LightningFinished.
// Only works for physical import.
func finishAndWait(ctx context.Context, cli *clientv3.Client, cfg *config.SubTaskConfig) error {
	return putAndWait(ctx, cli, cfg, ha.LightningFinished, func(s string) bool {
		return s != ha.LightningFinished
	})
}

func putAndWait(
	ctx context.Context,
	cli *clientv3.Client,
	cfg *config.SubTaskConfig,
	putStatus string,
	failFn func(string) bool,
) error {
	if cli == nil || cfg.LoaderConfig.ImportMode != config.LoadModePhysical {
		return nil
	}
	_, err := ha.PutLightningStatus(cli, cfg.Name, cfg.SourceID, putStatus)
	if err != nil {
		return err
	}

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
WaitLoop:
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			status, err := ha.GetAllLightningStatus(cli, cfg.Name)
			if err != nil {
				return err
			}
			for _, s := range status {
				if failFn(s) {
					continue WaitLoop
				}
			}
			return nil
		}
	}
}
