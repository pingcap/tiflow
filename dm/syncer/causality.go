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
	"time"

	"go.uber.org/zap"

	"github.com/pingcap/ticdc/dm/pkg/log"
	"github.com/pingcap/ticdc/dm/syncer/metrics"
)

// causality provides a simple mechanism to improve the concurrency of SQLs execution under the premise of ensuring correctness.
// causality groups sqls that maybe contain causal relationships, and syncer executes them linearly.
// if some conflicts exist in more than one groups, causality generate a conflict job and reset.
// this mechanism meets quiescent consistency to ensure correctness.
type causality struct {
	relations map[string]string
	outCh     chan *job
	inCh      chan *job
	logger    log.Logger

	// for metrics
	task   string
	source string
}

// causalityWrap creates and runs a causality instance.
func causalityWrap(inCh chan *job, syncer *Syncer) chan *job {
	causality := &causality{
		relations: make(map[string]string),
		task:      syncer.cfg.Name,
		source:    syncer.cfg.SourceID,
		logger:    syncer.tctx.Logger.WithFields(zap.String("component", "causality")),
		inCh:      inCh,
		outCh:     make(chan *job, syncer.cfg.QueueSize),
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
		if j.tp == flush {
			c.reset()
		} else {
			keys := j.dml.identifyKeys()
			// detectConflict before add
			if c.detectConflict(keys) {
				c.logger.Debug("meet causality key, will generate a conflict job to flush all sqls", zap.Strings("keys", keys))
				c.outCh <- newConflictJob()
				c.reset()
			}
			j.dml.key = c.add(keys)
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
func (c *causality) add(keys []string) string {
	if len(keys) == 0 {
		return ""
	}

	// find causal key
	selectedRelation := keys[0]
	var nonExistKeys []string
	for _, key := range keys {
		if val, ok := c.relations[key]; ok {
			selectedRelation = val
		} else {
			nonExistKeys = append(nonExistKeys, key)
		}
	}
	// set causal relations for those non-exist keys
	for _, key := range nonExistKeys {
		c.relations[key] = selectedRelation
	}

	return selectedRelation
}

// reset resets relations.
func (c *causality) reset() {
	c.relations = make(map[string]string)
}

// detectConflict detects whether there is a conflict.
func (c *causality) detectConflict(keys []string) bool {
	if len(keys) == 0 {
		return false
	}

	var existedRelation string
	for _, key := range keys {
		if val, ok := c.relations[key]; ok {
			if existedRelation != "" && val != existedRelation {
				return true
			}
			existedRelation = val
		}
	}

	return false
}
