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
	"time"

	"github.com/pingcap/tiflow/dm/pb"
	"github.com/pingcap/tiflow/dm/pkg/binlog"
	"go.uber.org/zap"
)

// Status implements Unit.Status.
func (l *Loader) Status(_ *binlog.SourceStatus) interface{} {
	finishedSize := l.finishedDataSize.Load()
	totalSize := l.totalDataSize.Load()
	progress := percent(finishedSize, totalSize, l.finish.Load())
	currentSpeed := int64(l.speedRecorder.GetSpeed(float64(finishedSize)))

	s := &pb.LoadStatus{
		FinishedBytes:  finishedSize,
		TotalBytes:     totalSize,
		Progress:       progress,
		MetaBinlog:     l.metaBinlog.Load(),
		MetaBinlogGTID: l.metaBinlogGTID.Load(),
		Bps:            currentSpeed,
	}
	go l.printStatus()
	return s
}

// printStatus prints status like progress percentage.
func (l *Loader) printStatus() {
	finishedSize := l.finishedDataSize.Load()
	totalSize := l.totalDataSize.Load()
	totalFileCount := l.totalFileCount.Load()

	interval := time.Since(l.dbTableDataLastUpdatedTime.Load())
	intervalSecond := interval.Seconds()
	if intervalSecond == 0 {
		return
	}

	for db, tables := range l.dbTableDataFinishedSize {
		for table, size := range tables {
			curFinished := size.Load()
			lastFinished := l.dbTableDataFinishedSize[db][table].Load()
			speed := float64(curFinished-lastFinished) / intervalSecond
			l.dbTableDataLastFinishedSize[db][table].Store(curFinished)
			if speed > 0 {
				remainingSeconds := float64(l.dbTableDataTotalSize[db][table].Load()-curFinished) / speed
				remainingTimeGauge.WithLabelValues(l.cfg.Name, l.cfg.WorkerName, l.cfg.SourceID, db, table).Set(remainingSeconds)
			}
		}
	}
	l.dbTableDataLastUpdatedTime.Store(time.Now())

	l.logger.Info("progress status of load",
		zap.Int64("finished_bytes", finishedSize),
		zap.Int64("total_bytes", totalSize),
		zap.Int64("total_file_count", totalFileCount),
		zap.String("progress", percent(finishedSize, totalSize, l.finish.Load())))
	progressGauge.WithLabelValues(l.cfg.Name, l.cfg.SourceID).Set(progress(finishedSize, totalSize, l.finish.Load()))
}
