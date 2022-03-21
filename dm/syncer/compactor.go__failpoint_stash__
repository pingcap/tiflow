// Copyright 2021 PingCAP, Inc.
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
	"fmt"
	"strconv"
	"time"

	"github.com/pingcap/failpoint"
	"go.uber.org/zap"

	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/dm/syncer/metrics"
	"github.com/pingcap/tiflow/pkg/sqlmodel"
)

// compactor compacts multiple statements into one statement.
type compactor struct {
	inCh       chan *job
	outCh      chan *job
	bufferSize int
	logger     log.Logger
	safeMode   bool

	keyMap map[string]map[string]int // table -> key(pk or (uk + not null)) -> index in buffer
	buffer []*job

	// for metrics
	task               string
	source             string
	updateJobMetricsFn func(bool, string, *job)
}

// compactorWrap creates and runs a compactor instance.
func compactorWrap(inCh chan *job, syncer *Syncer) chan *job {
	// Actually we can use a larger compact buffer-size, but if so, when user pause-task/stop-task, they may need to wait a longer time to wait all jobs flushed.
	// TODO: implement ping-pong buffer.
	bufferSize := syncer.cfg.QueueSize * syncer.cfg.WorkerCount / 4
	compactor := &compactor{
		inCh:               inCh,
		outCh:              make(chan *job, bufferSize),
		bufferSize:         bufferSize,
		logger:             syncer.tctx.Logger.WithFields(zap.String("component", "compactor")),
		keyMap:             make(map[string]map[string]int),
		buffer:             make([]*job, 0, bufferSize),
		task:               syncer.cfg.Name,
		source:             syncer.cfg.SourceID,
		updateJobMetricsFn: syncer.updateJobMetrics,
	}
	go func() {
		compactor.run()
		compactor.close()
	}()
	return compactor.outCh
}

// run runs a compactor instance.
func (c *compactor) run() {
	for {
		select {
		case j, ok := <-c.inCh:
			if !ok {
				return
			}
			metrics.QueueSizeGauge.WithLabelValues(c.task, "compactor_input", c.source).Set(float64(len(c.inCh)))

			if j.tp == flush || j.tp == asyncFlush {
				c.flushBuffer()
				c.outCh <- j
				continue
			}

			if j.tp == gc {
				c.outCh <- j
				continue
			}

			// set safeMode when receive first job
			if len(c.buffer) == 0 {
				c.safeMode = j.safeMode
			}
			// if dml has no PK/NOT NULL UK, do not compact it.
			if !j.dml.HasNotNullUniqueIdx() {
				c.buffer = append(c.buffer, j)
				continue
			}

			// if update job update its identify keys, turn it into delete + insert
			if j.dml.IsIdentityUpdated() {
				delDML, insertDML := j.dml.SplitUpdate()
				delJob := j.clone()
				delJob.dml = delDML

				insertJob := j.clone()
				insertJob.dml = insertDML

				c.compactJob(delJob)
				c.compactJob(insertJob)
			} else {
				c.compactJob(j)
			}

			failpoint.Inject("SkipFlushCompactor", func() {
				failpoint.Continue()
			})
			// if the number of outer jobs is zero or buffer is full, flush the buffer
			if len(c.outCh) == 0 || len(c.buffer) >= c.bufferSize {
				c.flushBuffer()
			}
			// if no inner jobs and the number of outer jobs is zero, flush the buffer
		case <-time.After(waitTime):
			failpoint.Inject("SkipFlushCompactor", func() {
				failpoint.Continue()
			})
			c.flushBuffer()
		}
	}
}

// close closes outer channels.
func (c *compactor) close() {
	close(c.outCh)
}

// flushBuffer flush buffer and reset compactor.
func (c *compactor) flushBuffer() {
	for _, j := range c.buffer {
		if j != nil {
			// set safemode for all jobs by first job in buffer.
			// or safemode for insert(delete + insert = insert with safemode)
			j.safeMode = c.safeMode || j.safeMode
			c.outCh <- j
		}
	}
	c.keyMap = make(map[string]map[string]int)
	c.buffer = c.buffer[0:0]
}

// compactJob compact jobs.
// INSERT + INSERT => X			‾|
// UPDATE + INSERT => X			 |=> DELETE + INSERT => INSERT ON DUPLICATE KEY UPDATE(REPLACE)
// DELETE + INSERT => REPLACE	_|
// INSERT + DELETE => DELETE	‾|
// UPDATE + DELETE => DELETE	 |=> anything + DELETE => DELETE
// DELETE + DELETE => X			_|
// INSERT + UPDATE => INSERT	‾|
// UPDATE + UPDATE => UPDATE	 |=> INSERT + UPDATE => INSERT, UPDATE + UPDATE => UPDATE
// DELETE + UPDATE => X			_|
// .
func (c *compactor) compactJob(j *job) {
	tableName := j.dml.TargetTableID()
	tableKeyMap, ok := c.keyMap[tableName]
	if !ok {
		// do not alloc a large buffersize, otherwise if the downstream latency is low
		// compactor will constantly flush the buffer and golang gc will affect performance
		c.keyMap[tableName] = make(map[string]int)
		tableKeyMap = c.keyMap[tableName]
	}

	key := j.dml.IdentityKey()

	failpoint.Inject("DownstreamIdentifyKeyCheckInCompact", func(v failpoint.Value) {
		value, err := strconv.Atoi(key)
		upper := v.(int)
		if err != nil || value > upper {
			panic(fmt.Sprintf("downstream identifyKey check failed. key value %v should less than %v", value, upper))
		}
	})

	prevPos, ok := tableKeyMap[key]
	// if no such key in the buffer, add it
	if !ok {
		tableKeyMap[key] = len(c.buffer)
		c.buffer = append(c.buffer, j)
		return
	}

	prevJob := c.buffer[prevPos]
	c.logger.Debug("start to compact", zap.Stringer("previous dml", prevJob.dml), zap.Stringer("current dml", j.dml))

	// adjust safemode
	adjustSafeMode(j, prevJob)
	if !shouldSkipReduce(j, prevJob) {
		j.dml.Reduce(prevJob.dml)
	}

	// mark previous job as compacted(nil), add new job
	c.buffer[prevPos] = nil
	tableKeyMap[key] = len(c.buffer)
	c.buffer = append(c.buffer, j)
	c.logger.Debug("finish to compact", zap.Stringer("dml", j.dml))
	c.updateJobMetricsFn(true, adminQueueName, newCompactJob(prevJob.targetTable))
}

func shouldSkipReduce(j, prevJob *job) bool {
	return j.dml.Type() == sqlmodel.RowChangeInsert &&
		prevJob.dml.Type() == sqlmodel.RowChangeDelete
}

func adjustSafeMode(j, prevJob *job) {
	switch j.dml.Type() {
	case sqlmodel.RowChangeUpdate:
		if prevJob.dml.Type() == sqlmodel.RowChangeInsert {
			// DELETE + INSERT + UPDATE => INSERT with safemode
			j.safeMode = prevJob.safeMode
		}
	case sqlmodel.RowChangeInsert:
		if prevJob.dml.Type() == sqlmodel.RowChangeDelete {
			j.safeMode = true
		}
	}
}
