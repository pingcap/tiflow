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
	"github.com/pingcap/ticdc/dm/pkg/terror"
)

// causality provides a simple mechanism to improve the concurrency of SQLs execution under the premise of ensuring correctness.
// causality groups sqls that maybe contain causal relationships, and syncer executes them linearly.
// if some conflicts exist in more than one groups, then syncer waits all SQLs that are grouped be executed and reset causality.
// this mechanism meets quiescent consistency to ensure correctness.
type causality struct {
	relations map[string]string
}

func newCausality() *causality {
	return &causality{
		relations: make(map[string]string),
	}
}

func (c *causality) add(keys []string) error {
	if len(keys) == 0 {
		return nil
	}

	if c.detectConflict(keys) {
		return terror.ErrSyncUnitCausalityConflict.Generate()
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
	return nil
}

func (c *causality) get(key string) string {
	return c.relations[key]
}

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
