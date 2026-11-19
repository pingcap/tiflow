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
// See the License for the specific language governing permissions and
// limitations under the License.

package syncer

import (
	"context"
	"errors"
	"math"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/pingcap/tiflow/dm/config"
	"github.com/pingcap/tiflow/dm/pkg/binlog"
	"github.com/pingcap/tiflow/dm/pkg/conn"
	tcontext "github.com/pingcap/tiflow/dm/pkg/context"
	"github.com/pingcap/tiflow/dm/pkg/gtid"
	"github.com/pingcap/tiflow/dm/pkg/schema"
	"github.com/pingcap/tiflow/dm/syncer/metrics"
	"github.com/pingcap/tiflow/engine/pkg/promutil"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

func TestUpdateSyncerBinlogMetrics(t *testing.T) {
	gtidSet, err := gtid.ParserGTID(
		mysql.MySQLFlavor,
		"3ccc475b-2343-11e7-be21-6c0b84d59f30:1-3",
	)
	require.NoError(t, err)

	testCases := []struct {
		name         string
		checkpoint   binlog.Location
		expectedFile float64
		expectedPos  float64
	}{
		{
			name: "file position checkpoint",
			checkpoint: binlog.NewLocation(mysql.Position{
				Name: "binary-log.346652",
				Pos:  560567,
			}, nil),
			expectedFile: 346652,
			expectedPos:  560567,
		},
		{
			name:         "fresh checkpoint with empty binlog filename",
			checkpoint:   binlog.MustZeroLocation(mysql.MySQLFlavor),
			expectedFile: math.NaN(),
			expectedPos:  float64(binlog.MinPosition.Pos),
		},
		{
			name: "checkpoint with malformed binlog filename",
			checkpoint: binlog.NewLocation(mysql.Position{
				Name: "not-a-binlog-name",
				Pos:  123,
			}, nil),
			expectedFile: math.NaN(),
			expectedPos:  123,
		},
		{
			name: "GTID-only checkpoint",
			checkpoint: binlog.NewLocation(
				mysql.Position{},
				gtidSet,
			),
			expectedFile: math.NaN(),
			expectedPos:  0,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			fileGauge := prometheus.NewGauge(prometheus.GaugeOpts{Name: "syncer_binlog_file"})
			posGauge := prometheus.NewGauge(prometheus.GaugeOpts{Name: "syncer_binlog_pos"})
			s := &Syncer{
				tctx: tcontext.Background(),
				metricsProxies: &metrics.Proxies{
					Metrics: &metrics.Metrics{
						BinlogSyncerFileGauge: fileGauge,
						BinlogSyncerPosGauge:  posGauge,
					},
				},
			}

			s.updateSyncerBinlogMetrics(tc.checkpoint)

			requireSyncerBinlogMetrics(t, s, tc.expectedFile, tc.expectedPos)
		})
	}
}

func TestSyncerBinlogMetricsLifecycle(t *testing.T) {
	t.Run("Init publishes persisted checkpoint", func(t *testing.T) {
		persisted := binlog.NewLocation(mysql.Position{
			Name: "binary-log.346652",
			Pos:  560567,
		}, nil)
		checkpoint := &binlogMetricsLifecycleCheckpoint{
			loadPoint: persisted,
		}
		s, mock := newSyncerForBinlogMetricsLifecycleTest(t, checkpoint, nil)

		require.Equal(t, 1, checkpoint.loadCalls)
		requireSyncerBinlogMetrics(t, s, 346652, 560567)
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("Run refreshes checkpoint selected from metadata", func(t *testing.T) {
		metaPoint := binlog.NewLocation(mysql.Position{
			Name: "mysql-bin.000123",
			Pos:  456,
		}, nil)
		stopAfterMetrics := errors.New("stop after updating syncer binlog metrics")
		checkpoint := &binlogMetricsLifecycleCheckpoint{
			loadPoint:                binlog.MustZeroLocation(mysql.MySQLFlavor),
			metaPoint:                metaPoint,
			loadIntoSchemaTrackerErr: stopAfterMetrics,
		}
		meta := &config.Meta{
			BinLogName: metaPoint.Position.Name,
			BinLogPos:  metaPoint.Position.Pos,
		}
		s, mock := newSyncerForBinlogMetricsLifecycleTest(t, checkpoint, meta)
		requireSyncerBinlogMetrics(t, s, math.NaN(), float64(binlog.MinPosition.Pos))

		mock.ExpectQuery("SELECT UNIX_TIMESTAMP\\(\\)").
			WillReturnRows(sqlmock.NewRows([]string{"UNIX_TIMESTAMP()"}).AddRow(123456789))
		err := s.Run(context.Background())
		require.ErrorIs(t, err, stopAfterMetrics)

		require.Equal(t, 1, checkpoint.loadMetaCalls)
		requireSyncerBinlogMetrics(t, s, 123, 456)
		require.NoError(t, mock.ExpectationsWereMet())
	})
}

type binlogMetricsLifecycleCheckpoint struct {
	CheckPoint

	globalPoint              binlog.Location
	loadPoint                binlog.Location
	metaPoint                binlog.Location
	loadIntoSchemaTrackerErr error
	loadCalls                int
	loadMetaCalls            int
}

func (c *binlogMetricsLifecycleCheckpoint) Init(*tcontext.Context) error {
	return nil
}

func (c *binlogMetricsLifecycleCheckpoint) Close() {}

func (c *binlogMetricsLifecycleCheckpoint) Load(*tcontext.Context) error {
	c.loadCalls++
	c.globalPoint = c.loadPoint
	return nil
}

func (c *binlogMetricsLifecycleCheckpoint) LoadMeta(context.Context) error {
	c.loadMetaCalls++
	c.globalPoint = c.metaPoint
	return nil
}

func (c *binlogMetricsLifecycleCheckpoint) GlobalPoint() binlog.Location {
	return c.globalPoint
}

func (c *binlogMetricsLifecycleCheckpoint) TablePoint() map[string]map[string]binlog.Location {
	return nil
}

func (c *binlogMetricsLifecycleCheckpoint) Rollback() {}

func (c *binlogMetricsLifecycleCheckpoint) DiscardPendingSnapshots() {}

func (c *binlogMetricsLifecycleCheckpoint) LoadIntoSchemaTracker(context.Context, *schema.Tracker) error {
	return c.loadIntoSchemaTrackerErr
}

func newSyncerForBinlogMetricsLifecycleTest(
	t *testing.T,
	checkpoint *binlogMetricsLifecycleCheckpoint,
	meta *config.Meta,
) (*Syncer, sqlmock.Sqlmock) {
	t.Helper()

	originalDBProvider := conn.DefaultDBProvider
	conn.DefaultDBProvider = &conn.DefaultDBProviderImpl{}
	db, mock, err := conn.InitMockDBNotClose()
	require.NoError(t, err)
	t.Cleanup(func() {
		conn.DefaultDBProvider = originalDBProvider
	})
	t.Cleanup(func() {
		_ = db.Close()
	})
	mock.MatchExpectationsInOrder(false)
	mock.ExpectQuery("SELECT cast\\(TIMEDIFF\\(NOW\\(6\\), UTC_TIMESTAMP\\(6\\)\\) as time\\);").
		WillReturnRows(sqlmock.NewRows([]string{""}).AddRow("00:00:00"))
	mock.ExpectQuery("SELECT @@lower_case_table_names;").
		WillReturnRows(sqlmock.NewRows([]string{"@@lower_case_table_names"}).AddRow(1))

	cfg := genDefaultSubTaskConfig4Test()
	cfg.LoaderConfig.Dir = t.TempDir()
	cfg.WorkerCount = 1
	cfg.QueueSize = 16
	cfg.Batch = 1
	cfg.Timezone = "UTC"
	cfg.SourceID = "source"
	cfg.WorkerName = "worker"
	cfg.Meta = meta
	cfg.To.Session = map[string]string{"sql_mode": "STRICT_TRANS_TABLES"}
	cfg.MetricsFactory = promutil.NewPromFactory()

	s := NewSyncer(cfg, nil, nil)
	s.checkpoint = checkpoint
	require.NoError(t, s.Init(context.Background()))
	t.Cleanup(s.Close)
	return s, mock
}

func requireSyncerBinlogMetrics(t *testing.T, s *Syncer, expectedFile, expectedPos float64) {
	t.Helper()

	actualFile := testutil.ToFloat64(s.metricsProxies.Metrics.BinlogSyncerFileGauge)
	if math.IsNaN(expectedFile) {
		require.True(t, math.IsNaN(actualFile))
	} else {
		require.Equal(t, expectedFile, actualFile)
	}
	require.Equal(t, expectedPos, testutil.ToFloat64(s.metricsProxies.Metrics.BinlogSyncerPosGauge))
}
