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
	"crypto/sha1"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strings"

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

// SQLReplace works like strings.Replace but only supports one replacement.
// It uses backquote pairs to quote the old and new word.
func SQLReplace(s, oldStr, newStr string, ansiquote bool) string {
	var quote string
	if ansiquote {
		quote = "\""
	} else {
		quote = "`"
	}
	quoteF := func(s string) string {
		var b strings.Builder
		b.WriteString(quote)
		b.WriteString(s)
		b.WriteString(quote)
		return b.String()
	}

	oldStr = quoteF(oldStr)
	newStr = quoteF(newStr)
	return strings.Replace(s, oldStr, newStr, 1)
}

// shortSha1 returns the first 6 characters of sha1 value.
func shortSha1(s string) string {
	h := sha1.New()

	h.Write([]byte(s))
	return fmt.Sprintf("%x", h.Sum(nil))[:6]
}

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

// progress calculates progress of a/b.
func progress(a int64, b int64, finish bool) float64 {
	if b == 0 {
		if finish {
			return 1
		}
		return 0
	}
	return float64(a) / float64(b)
}

func generateSchemaCreateFile(dir string, schema string) error {
	file, err := os.Create(path.Join(dir, fmt.Sprintf("%s-schema-create.sql", schema)))
	if err != nil {
		return terror.ErrLoadUnitCreateSchemaFile.Delegate(err)
	}
	defer file.Close()

	_, err = fmt.Fprintf(file, "CREATE DATABASE `%s`;\n", escapeName(schema))
	return terror.ErrLoadUnitCreateSchemaFile.Delegate(err)
}

func escapeName(name string) string {
	return strings.ReplaceAll(name, "`", "``")
}

// input filename is like `all_mode.t1.0.sql` or `all_mode.t1.sql`.
func getDBAndTableFromFilename(filename string) (string, string, error) {
	idx := strings.LastIndex(filename, ".sql")
	if idx < 0 {
		return "", "", fmt.Errorf("%s doesn't have a `.sql` suffix", filename)
	}
	fields := strings.Split(filename[:idx], ".")
	if len(fields) != 2 && len(fields) != 3 {
		return "", "", fmt.Errorf("%s doesn't have correct `.` separator", filename)
	}
	return fields[0], fields[1], nil
}

func getMydumpMetadata(ctx context.Context, cli *clientv3.Client, cfg *config.SubTaskConfig, workerName string) (string, string, error) {
	metafile := "metadata"
	failpoint.Inject("TestRemoveMetaFile", func() {
		err := storage.RemoveAll(ctx, cfg.LoaderConfig.Dir, nil)
		if err != nil {
			log.L().Warn("TestRemoveMetaFile Error", log.ShortError(err))
		}
	})
	loc, _, err := dumpling.ParseMetaData(ctx, cfg.LoaderConfig.Dir, metafile, cfg.Flavor, cfg.ExtStorage)
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
