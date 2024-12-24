// Copyright 2024 PingCAP, Inc.
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

package config

import (
	"testing"

	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/redo"
	"github.com/stretchr/testify/require"
)

func TestConsistentConfig(t *testing.T) {
	config := ConsistentConfig{
		Level:             string(redo.ConsistentLevelEventual),
		MaxLogSize:        -1,
		EncodingWorkerNum: -1,
		FlushWorkerNum:    -1,
	}
	config.ValidateAndAdjust()
	require.Equal(t, config.MaxLogSize, redo.DefaultMaxLogSize)
	require.EqualValues(t, config.FlushIntervalInMs, redo.DefaultFlushIntervalInMs)
	require.EqualValues(t, config.MetaFlushIntervalInMs, redo.DefaultMetaFlushIntervalInMs)
	require.EqualValues(t, config.EncodingWorkerNum, redo.DefaultEncodingWorkerNum)
	require.EqualValues(t, config.FlushWorkerNum, redo.DefaultFlushWorkerNum)

	config.EncodingWorkerNum = redo.MaxEncodingWorkerNum + 10
	config.FlushWorkerNum = redo.MaxFlushWorkerNum + 10
	config.ValidateAndAdjust()
	require.EqualValues(t, config.EncodingWorkerNum, redo.MaxEncodingWorkerNum)
	require.EqualValues(t, config.FlushWorkerNum, redo.MaxFlushWorkerNum)

	config.FlushIntervalInMs = -1
	require.ErrorIs(t, config.ValidateAndAdjust(), cerror.ErrInvalidReplicaConfig)
	config.FlushIntervalInMs = 0

	config.MetaFlushIntervalInMs = -1
	require.ErrorIs(t, config.ValidateAndAdjust(), cerror.ErrInvalidReplicaConfig)
	config.MetaFlushIntervalInMs = 0

	config.Compression = "compress"
	require.ErrorIs(t, config.ValidateAndAdjust(), cerror.ErrInvalidReplicaConfig)
	config.Compression = ""
}
