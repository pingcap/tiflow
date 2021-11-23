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

	"github.com/pingcap/ticdc/dm/pkg/log"
	"github.com/pingcap/ticdc/dm/syncer/metrics"
	"github.com/pingcap/tidb/sessionctx"
)

// causality provides a simple mechanism to improve the concurrency of SQLs execution under the premise of ensuring correctness.
// causality groups sqls that maybe contain causal relationships, and syncer executes them linearly.
// if some conflicts exist in more than one groups, causality generate a conflict job and reset.
// this mechanism meets quiescent consistency to ensure correctness.
type causality struct {
	relations *rollingMap
	outCh     chan *job
	inCh      chan *job
	logger    log.Logger
	sessCtx   sessionctx.Context

	// for metrics
	task   string
	source string
}

// causalityWrap creates and runs a causality instance.
func causalityWrap(inCh chan *job, syncer *Syncer) chan *job {
	causality := &causality{
		relations: newRollingMap(),
		task:      syncer.cfg.Name,
		source:    syncer.cfg.SourceID,
		logger:    syncer.tctx.Logger.WithFields(zap.String("component", "causality")),
		inCh:      inCh,
		outCh:     make(chan *job, syncer.cfg.QueueSize),
		sessCtx:   syncer.sessCtx,
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
			c.relations.rotate()
		case gc:
			// gc is only used on inner-causality logic
			c.relations.gc(j.seq)
			continue
		default:
			keys := j.dml.identifyKeys(c.sessCtx)

			// detectConflict before add
			if c.detectConflict(keys) {
				c.logger.Debug("meet causality key, will generate a conflict job to flush all sqls", zap.Strings("keys", keys))
				c.outCh <- newConflictJob()
				c.relations.clear()
			}
			j.dml.key = c.add(keys, j.seq)
			c.logger.Debug("key for keys", zap.String("key", j.dml.key), zap.Strings("keys", keys))
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
func (c *causality) add(keys []string, version int64) string {
	if len(keys) == 0 {
		return ""
	}

	// find causal key
	selectedRelation := keys[0]
	var nonExistKeys []string
	for _, key := range keys {
		if val, ok := c.relations.get(key); ok {
			selectedRelation = val
		} else {
			nonExistKeys = append(nonExistKeys, key)
		}
	}
	// set causal relations for those non-exist keys
	for _, key := range nonExistKeys {
		c.relations.set(key, selectedRelation, version)
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
		if val, ok := c.relations.get(key); ok {
			if existedRelation != "" && val != existedRelation {
				return true
			}
			existedRelation = val
		}
	}

	return false
}

type versionedMap struct {
	data   map[string]string
	maxVer int64
}

// rollingMap is a map this contains multi map instances.
type rollingMap struct {
	maps []*versionedMap
	// current map fro write
	cur *versionedMap
}

func newRollingMap() *rollingMap {
	m := &rollingMap{
		maps: make([]*versionedMap, 0),
	}
	m.rotate()
	return m
}

func (m *rollingMap) get(key string) (string, bool) {
	for i := len(m.maps) - 1; i >= 0; i-- {
		if v, ok := m.maps[i].data[key]; ok {
			return v, true
		}
	}
	return "", false
}

func (m *rollingMap) set(key string, val string, version int64) {
	m.cur.data[key] = val
	if m.cur.maxVer < version {
		m.cur.maxVer = version
	}
}

func (m *rollingMap) len() int {
	cnt := 0
	for _, d := range m.maps {
		cnt += len(d.data)
	}
	return cnt
}

func (m *rollingMap) rotate() {
	if len(m.maps) == 0 || len(m.maps[len(m.maps)-1].data) > 0 {
		m.maps = append(m.maps, &versionedMap{
			data: make(map[string]string),
		})
		m.cur = m.maps[len(m.maps)-1]
	}
}

func (m *rollingMap) clear() {
	m.gc(math.MaxInt64)
}

func (m *rollingMap) gc(version int64) {
	// nolint:ifshort
	idx := 0
	for i, d := range m.maps {
		if d.maxVer > 0 && d.maxVer <= version {
			// set nil value to trigger go gc
			m.maps[i] = nil
		} else {
			idx = i
			break
		}
	}

	if idx == len(m.maps)-1 {
		m.maps = m.maps[:0]
		m.rotate()
	} else if idx != 0 {
		m.maps = m.maps[idx:]
	}
}
