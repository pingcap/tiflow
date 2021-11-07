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

package orchestrator

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/pingcap/ticdc/pkg/etcd"
	"github.com/pingcap/ticdc/pkg/orchestrator/util"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/clientv3"
)

func setUpEtcd(t *testing.T) (func() *etcd.Client, func()) {
	dir := t.TempDir()
	err := os.Chmod(dir, 0o700)
	require.Nil(t, err)
	url, server, err := etcd.SetupEmbedEtcd(dir)
	require.Nil(t, err)
	endpoints := []string{url.String()}
	return func() *etcd.Client {
			rawCli, err := clientv3.NewFromURLs(endpoints)
			require.Nil(t, err)
			return etcd.Wrap(rawCli, map[string]prometheus.Counter{})
		}, func() {
			server.Close()
		}
}

func TestGetPatchGroup(t *testing.T) {
	t.Parallel()
	patchGroupSize := 1000
	patchGroup := make([][]DataPatch, patchGroupSize)
	for i := 0; i < patchGroupSize; i++ {
		patches := []DataPatch{&SingleDataPatch{
			Key: util.NewEtcdKey(fmt.Sprintf("/key%d", i)),
			Func: func(old []byte) (newValue []byte, changed bool, err error) {
				return nil, true, nil
			},
		}}
		patchGroup[i] = patches
	}
	for len(patchGroup) > 0 {
		batchPatches, n := getBatchPatches(patchGroup)
		require.LessOrEqual(t, len(batchPatches), maxBatchPatchSize)
		patchGroup = patchGroup[n:]
	}

	require.Equal(t, len(patchGroup), 0)
}

func TestGetBatchResponse(t *testing.T) {
	t.Parallel()
	newClient, closer := setUpEtcd(t)
	defer closer()

	cli := newClient()
	defer func() {
		_ = cli.Unwrap().Close()
	}()

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute*5)
	defer cancel()

	tickCounter := 0
	ticker := time.NewTicker(10 * time.Millisecond)
	prefix := "/getBatch"
	revision := int64(0)
	watchCh := cli.Watch(ctx, prefix, clientv3.WithPrefix(), clientv3.WithRev(revision+1))
	patchSize := 64

	// put batch to etcd
	go func() {
		for i := 0; i < 5; i++ {
			for j := 0; j < patchSize; j++ {
				time.Sleep(10 * time.Millisecond)
				_, err := cli.Put(ctx, prefix+fmt.Sprintf("/key%d", j), "abc")
				if err == nil || err.Error() == "etcdserver: request timed out" {
					continue
				}
			}
		}
	}()

	lastReceivedEventTime := time.Now()
	// simulate EtcdWorker run
RUN:
	for {
		responses := make([]clientv3.WatchResponse, 0)
		select {
		case <-ctx.Done():
			break RUN
		case <-ticker.C:
			if time.Since(lastReceivedEventTime) > etcdRequestProgressDuration {
				cancel()
			}
		case response := <-watchCh:
			err := response.Err()
			require.Nil(t, err)
			lastReceivedEventTime = time.Now()
			// Check whether the response is stale.
			if revision >= response.Header.GetRevision() {
				continue
			}
			revision = response.Header.GetRevision()
			// ProgressNotify implies no new events.
			if response.IsProgressNotify() {
				continue
			}
			responses = append(responses, response)
			batchResponses, rev, err := getBatchResponse(watchCh, revision)
			require.Nil(t, err)
			responses = append(responses, batchResponses...)
			revision = rev
			if tickCounter == 1 {
				// make sure batch successfully
				require.LessOrEqual(t, 2, len(responses))
			}
			tickCounter++
			// simulate time consume by reactor tick
			if tickCounter == 1 {
				time.Sleep(1 * time.Second)
			}
		}
	}
	require.Less(t, tickCounter, patchSize*5)
}
