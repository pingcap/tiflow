// Copyright 2023 PingCAP, Inc.
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

package memory

import (
	"context"
	"math"
	"sync"
	"time"

	"github.com/google/btree"
	"github.com/pingcap/tiflow/cdc/metadata"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/chann"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/util"
)

type key struct {
	s      string
	v      int
	target string
}

func keyLess(k1, k2 key) bool {
	return k1.s < k2.s || (k1.s == k2.s && k1.v > k2.v)
}

type storage struct {
	sync.RWMutex
	v int
	// map[key] -> string
	kvs btree.BTreeG[key]
}

func newStorage() *storage {
	return &storage{kvs: btree.NewG[key](16, keyLess)}
}

type captureOb struct {
	ctx context.Context
	s   *storage
	c   *metadata.CaptureInfo
}

func newCaptureOb(ctx context.Context, s *storage, c *metadata.CaptureInfo) *captureOb {
	return &captureOb{s: s, c: c}
}

func (c *captureOb) SelfCaptureInfo() *metadata.CaptureInfo {
	return c.c
}

func (c *captureOb) Heartbeat() metadata.Error {}

func (c *captureOb) TakeControl() metadata.ControllerObservation {
	// TODO: introduce a lease for owner.
	ownerKey := "tidb/cdc/default/__cdc_meta__/owner"
	for {
		c.s.Lock()
		if currOwner, exists := c.s.kvs.Get(key{s: ownerKey}); exists {
			c.s.Unlock()
			if currOwner.target == c.c.ID {
				return &controllerOb{c: c.SelfCaptureInfo()}
			} else {
				util.Hang(c.ctx, time.Second)
			}
		} else {
			c.s.kvs.ReplaceOrInsert(key{s: ownerKey, target: c.c.ID})
			c.s.Unlock()
			return &controllerOb{c: c.SelfCaptureInfo()}
		}
	}
}

func (c *captureOb) WatchOwners() <-chan metadata.OwnerTask {

}

type controllerOb struct {
	c *metadata.CaptureInfo
}
