// Copyright 2022 PingCAP, Inc.
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

package example

import (
	"context"
	"testing"
	"time"

	"github.com/pingcap/tiflow/engine/framework"
	frameModel "github.com/pingcap/tiflow/engine/framework/model"
	"github.com/pingcap/tiflow/pkg/logutil"
	"github.com/stretchr/testify/require"
)

func newExampleWorker() *exampleWorker {
	self := &exampleWorker{}
	self.BaseWorker = framework.MockBaseWorker(workerID, masterID, self)
	return self
}

func TestExampleWorker(t *testing.T) {
	t.Parallel()

	initLogger.Do(func() {
		_ = logutil.InitLogger(&logutil.Config{
			Level: "debug",
		})
	})

	worker := newExampleWorker()
	ctx := context.Background()
	err := worker.Init(ctx)
	require.NoError(t, err)

	// tick twice
	err = worker.Tick(ctx)
	require.NoError(t, err)
	err = worker.Tick(ctx)
	require.NoError(t, err)

	broker := worker.BaseWorker.(*framework.BaseWorkerForTesting).Broker
	defer broker.Close()
	broker.AssertPersisted(t, "/local/example")
	broker.AssertFileExists(t, workerID, "/local/example", "1.txt")
	broker.AssertFileExists(t, workerID, "/local/example", "2.txt")

	time.Sleep(time.Second)
	require.Eventually(t, func() bool {
		return worker.Status().State == frameModel.WorkerStateFinished
	}, time.Second, time.Millisecond*100)

	resp, err := worker.BaseWorker.MetaKVClient().Get(ctx, tickKey)
	require.NoError(t, err)
	require.Len(t, resp.Kvs, 1)
	require.Equal(t, "2", string(resp.Kvs[0].Value))

	framework.MockBaseWorkerCheckSendMessage(t, worker.BaseWorker.(*framework.BaseWorkerForTesting).DefaultBaseWorker, testTopic, testMsg)
	err = worker.Close(ctx)
	require.NoError(t, err)
}
