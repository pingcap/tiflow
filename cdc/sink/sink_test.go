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

package sink

import (
	"context"
	"testing"

	"github.com/pingcap/tiflow/pkg/config"
	"github.com/stretchr/testify/require"
)

func TestValidateSink(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	replicateConfig := config.GetDefaultReplicaConfig()

	// test sink uri error
	sinkURI := "mysql://root:111@127.0.0.1:3306/"
	err := Validate(ctx, sinkURI, replicateConfig)
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "fail to open MySQL connection")

	// test sink uri right
	sinkURI = "blackhole://"
	err = Validate(ctx, sinkURI, replicateConfig)
	require.Nil(t, err)

	// test bdr mode error
	replicateConfig.BDRMode = true
	sinkURI = "blackhole://"
	err = Validate(ctx, sinkURI, replicateConfig)
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "sink uri scheme is not supported in BDR mode")
}
