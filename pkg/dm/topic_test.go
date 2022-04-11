package dm

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTopic(t *testing.T) {
	t.Parallel()

	require.Equal(t, "operate-task-message-master-id-task-id", OperateTaskMessageTopic("master-id", "task-id"))
}
