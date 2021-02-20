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

package processor

import (
	"encoding/json"
	"math"
	"time"

	"github.com/pingcap/check"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/pkg/etcd"
)

type processorSuite struct{}

var _ = check.Suite(&processorSuite{})

func newProcessor4Test() *processor {
	changefeedID := "test-changefeed"
	p := newProcessor(nil, "test-changefeed", nil, &model.CaptureInfo{
		ID:            "test-captureID",
		AdvertiseAddr: "127.0.0.1:0000",
	})
	p.changefeed = newChangeFeedState(changefeedID, p.captureInfo.ID)
	p.changefeed.Info = &model.ChangeFeedInfo{
		SinkURI:    "blackhole://",
		CreateTime: time.Now(),
		StartTs:    0,
		TargetTs:   math.MaxUint64,
	}
	p.changefeed.Status = &model.ChangeFeedStatus{}
	return p
}

func (s *processorSuite) TestInitPosition(c *check.C) {
	p := newProcessor4Test()
	c.Assert(p.initPosition(), check.IsTrue)
	applyPatches(c, p.changefeed)
	c.Assert(p.changefeed.TaskPosition, check.DeepEquals,
		&model.TaskPosition{
			CheckPointTs: 0,
			ResolvedTs:   0,
			Count:        0,
			Error:        nil,
		})

	p = newProcessor4Test()
	p.changefeed.Info.StartTs = 66
	p.changefeed.Status.CheckpointTs = 88
	c.Assert(p.initPosition(), check.IsTrue)
	applyPatches(c, p.changefeed)
	c.Assert(p.changefeed.TaskPosition, check.DeepEquals,
		&model.TaskPosition{
			CheckPointTs: 88,
			ResolvedTs:   88,
			Count:        0,
			Error:        nil,
		})

	p = newProcessor4Test()
	p.changefeed.TaskPosition = &model.TaskPosition{
		CheckPointTs: 99,
		ResolvedTs:   99,
		Count:        0,
		Error:        nil,
	}
	c.Assert(p.initPosition(), check.IsFalse)
	applyPatches(c, p.changefeed)
	c.Assert(p.changefeed.TaskPosition, check.DeepEquals,
		&model.TaskPosition{
			CheckPointTs: 99,
			ResolvedTs:   99,
			Count:        0,
			Error:        nil,
		})
}

func applyPatches(c *check.C, state *changefeedState) {
	for _, patch := range state.pendingPatches {
		key := &etcd.CDCKey{}
		key.Parse(patch.Key.String())
		var value []byte
		var err error
		switch key.Tp {
		case etcd.CDCKeyTypeTaskPosition:
			if state.TaskPosition == nil {
				value = nil
				break
			}
			value, err = json.Marshal(state.TaskPosition)
			c.Assert(err, check.IsNil)
		default:
			c.FailNow()
		}
		newValue, err := patch.Fun(value)
		c.Assert(err, check.IsNil)
		err = state.UpdateCDCKey(key, newValue)
		c.Assert(err, check.IsNil)
	}
}
