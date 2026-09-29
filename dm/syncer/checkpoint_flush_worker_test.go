// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package syncer

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/util/filter"
	"github.com/pingcap/tiflow/dm/pkg/binlog"
	tcontext "github.com/pingcap/tiflow/dm/pkg/context"
	"github.com/pingcap/tiflow/dm/pkg/schema"
	"github.com/pingcap/tiflow/dm/pkg/terror"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
)

func TestCheckpointFlushWorkerSkipsCheckpointOnBeginError(t *testing.T) {
	t.Parallel()

	cp := &checkpointFlushWorkerTestCheckpoint{}
	var execError atomic.Error
	execError.Store(terror.ErrDBExecuteFailedBegin.Delegate(sql.ErrConnDone))

	worker := &checkpointFlushWorker{
		input:              make(chan *checkpointFlushTask, 1),
		cp:                 cp,
		execError:          &execError,
		afterFlushFn:       func(*checkpointFlushTask) error { return nil },
		updateJobMetricsFn: func(bool, string, *job) {},
	}

	done := make(chan struct{})
	go func() {
		worker.Run(tcontext.Background())
		close(done)
	}()

	errCh := make(chan error, 1)
	worker.Add(&checkpointFlushTask{
		snapshotInfo:   &SnapshotInfo{id: 1},
		syncFlushErrCh: errCh,
	})
	require.NoError(t, <-errCh)

	worker.Close()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("checkpoint flush worker did not stop")
	}
	require.Zero(t, cp.flushCount, "checkpoint flush must be skipped after downstream BeginTx failure; flushing here can persist a checkpoint past non-durable DML")
}

type checkpointFlushWorkerTestCheckpoint struct {
	flushCount int
}

func (c *checkpointFlushWorkerTestCheckpoint) Init(*tcontext.Context) error { return nil }

func (c *checkpointFlushWorkerTestCheckpoint) Close() {}

func (c *checkpointFlushWorkerTestCheckpoint) ResetConn(*tcontext.Context) error { return nil }

func (c *checkpointFlushWorkerTestCheckpoint) Clear(*tcontext.Context) error { return nil }

func (c *checkpointFlushWorkerTestCheckpoint) Load(*tcontext.Context) error { return nil }

func (c *checkpointFlushWorkerTestCheckpoint) LoadMeta(context.Context) error { return nil }

func (c *checkpointFlushWorkerTestCheckpoint) SaveTablePoint(*filter.Table, binlog.Location, *model.TableInfo) {
}

func (c *checkpointFlushWorkerTestCheckpoint) DeleteTablePoint(*tcontext.Context, *filter.Table) error {
	return nil
}

func (c *checkpointFlushWorkerTestCheckpoint) DeleteAllTablePoint(*tcontext.Context) error {
	return nil
}

func (c *checkpointFlushWorkerTestCheckpoint) DeleteSchemaPoint(*tcontext.Context, string) error {
	return nil
}

func (c *checkpointFlushWorkerTestCheckpoint) IsOlderThanTablePoint(*filter.Table, binlog.Location) bool {
	return false
}

func (c *checkpointFlushWorkerTestCheckpoint) SaveGlobalPoint(binlog.Location) {}

func (c *checkpointFlushWorkerTestCheckpoint) SaveGlobalPointForcibly(binlog.Location) {}

func (c *checkpointFlushWorkerTestCheckpoint) Snapshot(bool) *SnapshotInfo { return nil }

func (c *checkpointFlushWorkerTestCheckpoint) DiscardPendingSnapshots() {}

func (c *checkpointFlushWorkerTestCheckpoint) FlushPointsExcept(
	*tcontext.Context,
	int,
	[]*filter.Table,
	[]string,
	[][]interface{},
) error {
	c.flushCount++
	return nil
}

func (c *checkpointFlushWorkerTestCheckpoint) FlushPointsWithTableInfos(
	*tcontext.Context,
	[]*filter.Table,
	[]*model.TableInfo,
) error {
	return nil
}

func (c *checkpointFlushWorkerTestCheckpoint) FlushSafeModeExitPoint(*tcontext.Context) error {
	return nil
}

func (c *checkpointFlushWorkerTestCheckpoint) GlobalPoint() binlog.Location { return binlog.Location{} }

func (c *checkpointFlushWorkerTestCheckpoint) GlobalPointSaveTime() time.Time { return time.Time{} }

func (c *checkpointFlushWorkerTestCheckpoint) SaveSafeModeExitPoint(*binlog.Location) {}

func (c *checkpointFlushWorkerTestCheckpoint) SafeModeExitPoint() *binlog.Location { return nil }

func (c *checkpointFlushWorkerTestCheckpoint) TablePoint() map[string]map[string]binlog.Location {
	return nil
}

func (c *checkpointFlushWorkerTestCheckpoint) GetTableInfo(string, string) *model.TableInfo {
	return nil
}

func (c *checkpointFlushWorkerTestCheckpoint) FlushedGlobalPoint() binlog.Location {
	return binlog.Location{}
}

func (c *checkpointFlushWorkerTestCheckpoint) LastFlushOutdated() bool { return false }

func (c *checkpointFlushWorkerTestCheckpoint) Rollback() {}

func (c *checkpointFlushWorkerTestCheckpoint) String() string { return "" }

func (c *checkpointFlushWorkerTestCheckpoint) LoadIntoSchemaTracker(context.Context, *schema.Tracker) error {
	return nil
}

func (c *checkpointFlushWorkerTestCheckpoint) CheckAndUpdate(
	context.Context,
	map[string]string,
	map[string]map[string]string,
) error {
	return nil
}
