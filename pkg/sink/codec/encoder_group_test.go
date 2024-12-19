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

package codec

import (
	"testing"

	"github.com/pingcap/tidb/pkg/util/cpu"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/util"
	"github.com/stretchr/testify/require"
)

func TestEncoderConcurrency(t *testing.T) {
	cfg := config.GetDefaultReplicaConfig().Sink
	cfg.EncoderConcurrency = util.AddressOf(-1)
	eg := NewEncoderGroup(
		cfg,
		nil,
		model.ChangeFeedID4Test("", ""),
	)
	require.Equal(t, len(eg.inputCh), config.DefaultEncoderGroupConcurrency)

	limitConcurrency := cpu.GetCPUCount() * 10
	cfg.EncoderConcurrency = util.AddressOf(limitConcurrency + 10)
	eg = NewEncoderGroup(
		cfg,
		nil,
		model.ChangeFeedID4Test("", ""),
	)
	require.Equal(t, len(eg.inputCh), limitConcurrency)
}
