// Copyright 2020 PingCAP, Inc.
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
	"context"
	"os"
	"strconv"
	"time"

	"github.com/pingcap/check"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/util/testleak"
)

type backendPoolSuite struct{}

var _ = check.SerialSuites(&backendPoolSuite{})

func (s *backendPoolSuite) TestBasicFunction(c *check.C) {
	defer testleak.AfterTest(c)()

	err := os.MkdirAll("/tmp/sorter", 0o755)
	c.Assert(err, check.IsNil)

	conf := config.GetDefaultServerConfig()
	conf.Sorter.MaxMemoryPressure = 90                         // 90%
	conf.Sorter.MaxMemoryConsumption = 16 * 1024 * 1024 * 1024 // 16G
	config.StoreGlobalServerConfig(conf)

	err = failpoint.Enable("github.com/pingcap/ticdc/cdc/puller/sorter/memoryPressureInjectPoint", "return(100)")
	c.Assert(err, check.IsNil)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*20)
	defer cancel()

	backEndPool := newBackEndPool("/tmp/sorter", "")
	c.Assert(backEndPool, check.NotNil)
	defer backEndPool.terminate()

	backEnd, err := backEndPool.alloc(ctx)
	c.Assert(err, check.IsNil)
	c.Assert(backEnd, check.FitsTypeOf, &fileBackEnd{})
	fileName := backEnd.(*fileBackEnd).fileName
	c.Assert(fileName, check.Not(check.Equals), "")

	err = failpoint.Enable("github.com/pingcap/ticdc/cdc/puller/sorter/memoryPressureInjectPoint", "return(0)")
	c.Assert(err, check.IsNil)
	err = failpoint.Enable("github.com/pingcap/ticdc/cdc/puller/sorter/memoryUsageInjectPoint", "return(34359738368)")
	c.Assert(err, check.IsNil)

	backEnd1, err := backEndPool.alloc(ctx)
	c.Assert(err, check.IsNil)
	c.Assert(backEnd1, check.FitsTypeOf, &fileBackEnd{})
	fileName1 := backEnd1.(*fileBackEnd).fileName
	c.Assert(fileName1, check.Not(check.Equals), "")
	c.Assert(fileName1, check.Not(check.Equals), fileName)

	err = failpoint.Enable("github.com/pingcap/ticdc/cdc/puller/sorter/memoryPressureInjectPoint", "return(0)")
	c.Assert(err, check.IsNil)
	err = failpoint.Enable("github.com/pingcap/ticdc/cdc/puller/sorter/memoryUsageInjectPoint", "return(0)")
	c.Assert(err, check.IsNil)

	backEnd2, err := backEndPool.alloc(ctx)
	c.Assert(err, check.IsNil)
	c.Assert(backEnd2, check.FitsTypeOf, &memoryBackEnd{})

	err = backEndPool.dealloc(backEnd)
	c.Assert(err, check.IsNil)

	err = backEndPool.dealloc(backEnd1)
	c.Assert(err, check.IsNil)

	err = backEndPool.dealloc(backEnd2)
	c.Assert(err, check.IsNil)

	time.Sleep(backgroundJobInterval * 3 / 2)

	_, err = os.Stat(fileName)
	c.Assert(os.IsNotExist(err), check.IsTrue)

	_, err = os.Stat(fileName1)
	c.Assert(os.IsNotExist(err), check.IsTrue)
}

func (s *backendPoolSuite) TestCleanUp(c *check.C) {
	defer testleak.AfterTest(c)()

	err := os.MkdirAll("/tmp/sorter", 0o755)
	c.Assert(err, check.IsNil)

	conf := config.GetDefaultServerConfig()
	conf.Sorter.MaxMemoryPressure = 90                         // 90%
	conf.Sorter.MaxMemoryConsumption = 16 * 1024 * 1024 * 1024 // 16G
	config.StoreGlobalServerConfig(conf)

	err = failpoint.Enable("github.com/pingcap/ticdc/cdc/puller/sorter/memoryPressureInjectPoint", "return(100)")
	c.Assert(err, check.IsNil)

	backEndPool := newBackEndPool("/tmp/sorter", "")
	c.Assert(backEndPool, check.NotNil)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*20)
	defer cancel()

	var fileNames []string
	for i := 0; i < 20; i++ {
		backEnd, err := backEndPool.alloc(ctx)
		c.Assert(err, check.IsNil)
		c.Assert(backEnd, check.FitsTypeOf, &fileBackEnd{})

		fileName := backEnd.(*fileBackEnd).fileName
		_, err = os.Stat(fileName)
		c.Assert(err, check.IsNil)

		fileNames = append(fileNames, fileName)
	}

	prefix := backEndPool.filePrefix
	c.Assert(prefix, check.Not(check.Equals), "")

	for j := 100; j < 120; j++ {
		fileName := prefix + strconv.Itoa(j) + ".tmp"
		f, err := os.Create(fileName)
		c.Assert(err, check.IsNil)
		err = f.Close()
		c.Assert(err, check.IsNil)

		fileNames = append(fileNames, fileName)
	}

	backEndPool.terminate()

	for _, fileName := range fileNames {
		_, err = os.Stat(fileName)
		c.Assert(os.IsNotExist(err), check.IsTrue)
	}
}
