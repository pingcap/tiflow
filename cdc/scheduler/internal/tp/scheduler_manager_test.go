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

package tp

import (
	"testing"
	"time"

	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/stretchr/testify/require"
)

func TestNewSchedulerManager(t *testing.T) {
	t.Parallel()

	m := newSchedulerManager(model.DefaultChangeFeedID("test-changefeed"),
		config.TomlDuration(10*time.Minute))
	require.NotNil(t, m)
	require.NotNil(t, m.schedulers[schedulerTypeBasic])
	require.NotNil(t, m.schedulers[schedulerTypeBalance])
	require.NotNil(t, m.schedulers[schedulerTypeMoveTable])
	require.NotNil(t, m.schedulers[schedulerTypeRebalance])
	require.NotNil(t, m.schedulers[schedulerTypeDrainCapture])
}
