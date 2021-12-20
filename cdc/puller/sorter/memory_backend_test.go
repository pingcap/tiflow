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

package sorter

import (
	"runtime"
	"sync/atomic"
	"time"

	"github.com/pingcap/check"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/util/testleak"
)

type memoryBackendSuite struct{}

var _ = check.SerialSuites(&memoryBackendSuite{})

func (s *memoryBackendSuite) TestNoLeaking(c *check.C) {
	defer testleak.AfterTest(c)()

	bknd := newMemoryBackEnd()
	wrtr, err := bknd.writer()
	c.Assert(err, check.IsNil)

	var objCount int64
	for i := 0; i < 10000; i++ {
		atomic.AddInt64(&objCount, 1)
		event := model.NewResolvedPolymorphicEvent(0, 1)
		runtime.SetFinalizer(event, func(*model.PolymorphicEvent) {
			atomic.AddInt64(&objCount, -1)
		})
		err := wrtr.writeNext(event)
		c.Assert(err, check.IsNil)
	}
	err = wrtr.flushAndClose()
	c.Assert(err, check.IsNil)

	rdr, err := bknd.reader()
	c.Assert(err, check.IsNil)

	for i := 0; i < 5000; i++ {
		_, err := rdr.readNext()
		c.Assert(err, check.IsNil)
	}

	for i := 0; i < 10; i++ {
		runtime.GC()
		if atomic.LoadInt64(&objCount) <= 5000 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	c.Assert(atomic.LoadInt64(&objCount), check.LessEqual, int64(5000))

	err = rdr.resetAndClose()
	c.Assert(err, check.IsNil)

	for i := 0; i < 10; i++ {
		runtime.GC()
		if atomic.LoadInt64(&objCount) == 0 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	c.Assert(atomic.LoadInt64(&objCount), check.Equals, int64(0))
}
