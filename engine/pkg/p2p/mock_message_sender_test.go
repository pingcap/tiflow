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

package p2p

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

var _ MessageSender = (*MockMessageSender)(nil)

type msgForTesting struct {
	Val int
}

func TestMockMessageSender(t *testing.T) {
	sender := NewMockMessageSender()

	_, ok := sender.TryPop("dummy", "dummy")
	require.False(t, ok)

	for i := 0; i < 50; i++ {
		topicID := fmt.Sprintf("topic-%d", i)
		for j := 0; j < 50; j++ {
			nodeID := fmt.Sprintf("node-%d", j)
			for k := 0; k < 50; k++ {
				ok, err := sender.SendToNode(context.TODO(), nodeID, topicID, &msgForTesting{k})
				require.True(t, ok)
				require.NoError(t, err)
			}
		}
	}

	for i := 0; i < 50; i++ {
		topicID := fmt.Sprintf("topic-%d", i)
		for j := 0; j < 50; j++ {
			nodeID := fmt.Sprintf("node-%d", j)
			for k := 0; k < 50; k++ {
				msg, ok := sender.TryPop(nodeID, topicID)
				require.True(t, ok)
				require.Equal(t, &msgForTesting{k}, msg)
			}
			_, ok := sender.TryPop(nodeID, topicID)
			require.False(t, ok)
		}
	}
}
