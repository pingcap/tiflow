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

package syncer

import (
	"math"
	"time"

	"go.uber.org/zap"

	"github.com/pingcap/tidb/sessionctx"

	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/dm/syncer/metrics"
)

// causality provides a simple mechanism to improve the concurrency of SQLs execution under the premise of ensuring correctness.
// causality groups sqls that maybe contain causal relationships, and syncer executes them linearly.
// if some conflicts exist in more than one groups, causality generate a conflict job and reset.
// this mechanism meets quiescent consistency to ensure correctness.
// causality relation is consisted of groups of keys separated by flush job, and such design helps removed flushed dml job keys.
type causality struct {
	relation    *causalityRelation
	outCh       chan *job
	inCh        chan *job
	logger      log.Logger
	sessCtx     sessionctx.Context
	workerCount int

	// for metrics
	task   string
	source string
}

// causalityWrap creates and runs a causality instance.
func causalityWrap(inCh chan *job, syncer *Syncer) chan *job {
	causality := &causality{
		relation:    newCausalityRelation(),
		task:        syncer.cfg.Name,
		source:      syncer.cfg.SourceID,
		logger:      syncer.tctx.Logger.WithFields(zap.String("component", "causality")),
		inCh:        inCh,
		outCh:       make(chan *job, syncer.cfg.QueueSize),
		sessCtx:     syncer.sessCtx,
		workerCount: syncer.cfg.WorkerCount,
	}

	go func() {
		causality.run()
		causality.close()
	}()

	return causality.outCh
}

// run receives dml jobs and send causality jobs by adding causality key.
// When meet conflict, sends a conflict job.
func (c *causality) run() {
	for j := range c.inCh {
		metrics.QueueSizeGauge.WithLabelValues(c.task, "causality_input", c.source).Set(float64(len(c.inCh)))

		startTime := time.Now()

		switch j.tp {
		case flush, asyncFlush:
			c.relation.rotate(j.flushSeq)
		case gc:
			// gc is only used on inner-causality logic
			c.relation.gc(j.flushSeq)
			continue
		default:
			keys := j.dml.CausalityKeys()

			// detectConflict before add
			if c.detectConflict(keys) {
				c.logger.Debug("meet causality key, will generate a conflict job to flush all sqls", zap.Strings("keys", keys))
				c.outCh <- newConflictJob(c.workerCount)
				c.relation.clear()
			}
			j.dmlQueueKey = c.add(keys)
			c.logger.Debug("key for keys", zap.String("key", j.dmlQueueKey), zap.Strings("keys", keys))
		}
		metrics.ConflictDetectDurationHistogram.WithLabelValues(c.task, c.source).Observe(time.Since(startTime).Seconds())

		c.outCh <- j
	}
}

// close closes outer channel.
func (c *causality) close() {
	close(c.outCh)
}

// add adds keys relation and return the relation. The keys must `detectConflict` first to ensure correctness.
func (c *causality) add(keys []string) string {
	if len(keys) == 0 {
		return ""
	}

	// find causal key
	selectedRelation := keys[0]
	var nonExistKeys []string
	for _, key := range keys {
		if val, ok := c.relation.get(key); ok {
			selectedRelation = val
		} else {
			nonExistKeys = append(nonExistKeys, key)
		}
	}
	// set causal relations for those non-exist keys
	for _, key := range nonExistKeys {
		c.relation.set(key, selectedRelation)
	}

	return selectedRelation
}

// detectConflict detects whether there is a conflict.
func (c *causality) detectConflict(keys []string) bool {
	if len(keys) == 0 {
		return false
	}

	var existedRelation string
	for _, key := range keys {
		if val, ok := c.relation.get(key); ok {
			if existedRelation != "" && val != existedRelation {
				return true
			}
			existedRelation = val
		}
	}

	return false
}

// dmlJobKeyRelationGroup stores a group of dml job key relations as data, and a flush job seq representing last flush job before adding any job keys.
type dmlJobKeyRelationGroup struct {
	data            map[string]string
	prevFlushJobSeq int64
}

// causalityRelation stores causality keys by group, where each group created on each flush and it helps to remove stale causality keys.
type causalityRelation struct {
	groups []*dmlJobKeyRelationGroup
}

func newCausalityRelation() *causalityRelation {
	m := &causalityRelation{}
	m.rotate(-1)
	return m
}

func (m *causalityRelation) get(key string) (string, bool) {
	for i := len(m.groups) - 1; i >= 0; i-- {
		if v, ok := m.groups[i].data[key]; ok {
			return v, true
		}
	}
	return "", false
}

func (m *causalityRelation) set(key string, val string) {
	m.groups[len(m.groups)-1].data[key] = val
}

func (m *causalityRelation) len() int {
	cnt := 0
	for _, d := range m.groups {
		cnt += len(d.data)
	}
	return cnt
}

func (m *causalityRelation) rotate(flushJobSeq int64) {
	m.groups = append(m.groups, &dmlJobKeyRelationGroup{
		data:            make(map[string]string),
		prevFlushJobSeq: flushJobSeq,
	})
}

func (m *causalityRelation) clear() {
	m.gc(math.MaxInt64)
}

// remove group of keys where its group's prevFlushJobSeq is smaller than or equal with the given flushJobSeq.
func (m *causalityRelation) gc(flushJobSeq int64) {
	if flushJobSeq == math.MaxInt64 {
		m.groups = m.groups[:0]
		m.rotate(-1)
		return
	}

	// nolint:ifshort
	idx := 0
	for i, d := range m.groups {
		if d.prevFlushJobSeq <= flushJobSeq {
			idx = i
		} else {
			break
		}
	}

	m.groups = m.groups[idx:]
}
