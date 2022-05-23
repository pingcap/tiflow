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

package worker

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	libModel "github.com/pingcap/tiflow/engine/lib/model"
	"github.com/pingcap/tiflow/engine/pkg/clock"
	pkgOrm "github.com/pingcap/tiflow/engine/pkg/orm"
	"github.com/pingcap/tiflow/engine/pkg/p2p"
)

func TestMasterClientRefreshInfo(t *testing.T) {
	meta, err := pkgOrm.NewMockClient()
	require.NoError(t, err)

	err = meta.UpsertJob(context.Background(), &libModel.MasterMetaKVData{
		ID:         "master-1",
		StatusCode: libModel.MasterStatusInit,
		NodeID:     "executor-1",
		Addr:       "192.168.0.1:1234",
		Epoch:      1,
	})
	require.NoError(t, err)

	masterCli := NewMasterClient(
		"master-1",
		"worker-1",
		p2p.NewMockMessageSender(),
		meta,
		clock.MonoNow())
	err = masterCli.InitMasterInfoFromMeta(context.Background())
	require.NoError(t, err)

	nodeID, epoch := masterCli.getMasterInfo()
	require.Equal(t, "executor-1", nodeID)
	require.Equal(t, libModel.Epoch(1), epoch)

	err = meta.UpsertJob(context.Background(), &libModel.MasterMetaKVData{
		ID:         "master-1",
		StatusCode: libModel.MasterStatusInit,
		NodeID:     "executor-2",
		Addr:       "192.168.0.2:1234",
		Epoch:      2,
	})
	require.NoError(t, err)

	err = masterCli.SyncRefreshMasterInfo(context.Background())
	require.NoError(t, err)

	nodeID, epoch = masterCli.getMasterInfo()
	require.Equal(t, "executor-2", nodeID)
	require.Equal(t, libModel.Epoch(2), epoch)
}
