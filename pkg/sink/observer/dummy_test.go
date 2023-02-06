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

package observer

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDummyObserverCloseSequence(t *testing.T) {
	t.Parallel()

	observer := NewDummyObserver()
	err := observer.Close()
	require.NoError(t, err)
	err = observer.Tick(context.Background())
	require.NoError(t, err)
}
