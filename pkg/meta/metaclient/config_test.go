package metaclient

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestConfigClone(t *testing.T) {
	conf := Config{
		Endpoints: []string{"127.0.0.1:3000", "127.0.0.1:3001"},
		Auth: AuthConf{
			Username: "user1",
			Password: "password1",
		},
		/*
			Dial: DialConf{
				DialTimeout:    30 * time.Second,
				MaxRecvMsgSize: 11,
				DialOptions: []interface{}{
					grpc.WithBlock(),
					grpc.WithInsecure(),
				},
			},
		*/
		Log: LogConf{},
	}

	cloneConf := conf.Clone()
	require.Equal(t, conf.Endpoints, cloneConf.Endpoints)
	require.Len(t, cloneConf.Endpoints, 2)
	require.Equal(t, conf.Auth.Username, cloneConf.Auth.Username)
	require.Equal(t, conf.Auth.Password, cloneConf.Auth.Password)
	// require.Equal(t, conf.Dial, cloneConf.Dial)
	// require.IsType(t, grpc.WithMaxMsgSize(12), cloneConf.Dial.DialOptions[0])
}
