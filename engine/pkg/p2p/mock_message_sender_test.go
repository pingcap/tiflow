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
