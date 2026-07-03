// Copyright 2023 PingCAP, Inc.
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

package claimcheck

import (
	"context"
	"testing"

	"github.com/pingcap/tiflow/cdc/model"
	"github.com/stretchr/testify/require"
)

func TestClaimCheckFileName(t *testing.T) {
	ctx := context.Background()
	storageURI := "file:///tmp/abc/"
	changefeedID := model.DefaultChangeFeedID("test")

	claimCheck, err := New(ctx, storageURI, changefeedID)
	require.NoError(t, err)

	fileName := claimCheck.FileNameWithPrefix("file.json")
	require.Equal(t, "file:///tmp/abc/file.json", fileName)
}
