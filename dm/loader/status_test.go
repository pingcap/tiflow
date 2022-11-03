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

package loader

import (
	"sync"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/dumpling/export"
	"github.com/pingcap/tiflow/dm/config"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"go.uber.org/atomic"
)

func (*testLoaderSuite) TestConcurrentStatus(c *C) {
	l := &Loader{speedRecorder: export.NewSpeedRecorder()}
	l.cfg = &config.SubTaskConfig{}
	l.logger = log.L()
	l.finishedDataSize.Store(100)
	l.totalDataSize.Store(200)
	l.totalFileCount.Store(10)
	l.dbTableDataFinishedSize = map[string]map[string]*atomic.Int64{
		"db1": {
			"table1": atomic.NewInt64(10),
			"table2": atomic.NewInt64(20),
		},
	}
	l.dbTableDataLastFinishedSize = map[string]map[string]*atomic.Int64{
		"db1": {
			"table1": atomic.NewInt64(0),
			"table2": atomic.NewInt64(0),
		},
	}

	// test won't race or panic
	wg := sync.WaitGroup{}
	wg.Add(20)
	for i := 0; i < 20; i++ {
		go func() {
			l.Status(nil)
			wg.Done()
		}()
	}
	wg.Wait()
}
