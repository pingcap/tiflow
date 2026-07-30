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
	"math"
	"testing"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/pingcap/tiflow/dm/pkg/binlog"
	tcontext "github.com/pingcap/tiflow/dm/pkg/context"
	"github.com/pingcap/tiflow/dm/pkg/gtid"
	"github.com/pingcap/tiflow/dm/syncer/metrics"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

func TestInitSyncerBinlogMetrics(t *testing.T) {
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

			s.initSyncerBinlogMetrics(tc.checkpoint)

			actualFile := testutil.ToFloat64(fileGauge)
			if math.IsNaN(tc.expectedFile) {
				require.True(t, math.IsNaN(actualFile))
			} else {
				require.Equal(t, tc.expectedFile, actualFile)
			}
			require.Equal(t, tc.expectedPos, testutil.ToFloat64(posGauge))
		})
	}
}
