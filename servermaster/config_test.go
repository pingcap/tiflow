package servermaster

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMetaStoreConfig(t *testing.T) {
	t.Parallel()

	config := NewConfig()
	err := config.Parse([]string{
		"--master-addr",
		"0.0.0.0:10240",
		"--advertise-addr",
		"server-master:10240",
		"--peer-urls",
		"http://127.0.0.1:8291",
		"--advertise-peer-urls",
		"http://server-master:8291",
		"--frame-meta-endpoints",
		"frame-etcd-standalone:1111",
		"--frame-meta-user",
		"root134",
		"--frame-meta-password",
		"root123",
		"--user-meta-endpoints",
		"user-etcd-standalone:2222",
	})
	require.Nil(t, err)
	require.Regexp(t, "...:1111$", config.FrameMetaConf.Endpoints[0])
	require.Regexp(t, "root134", config.FrameMetaConf.Auth.User)
	require.Regexp(t, "root123", config.FrameMetaConf.Auth.Passwd)
	require.Regexp(t, "...:2222$", config.UserMetaConf.Endpoints[0])
}
