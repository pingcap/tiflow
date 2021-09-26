package sink

import (
	"context"
	"testing"

	"github.com/pingcap/ticdc/pkg/config"
	"github.com/stretchr/testify/require"
)

func TestValidateSink(t *testing.T) {
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	replicateConfig := config.GetDefaultReplicaConfig()
	opts := make(map[string]string)

	// test sink uri error
	sinkURI := "mysql://root:111@127.0.0.1:3306/"
	err := Validate(ctx, sinkURI, replicateConfig, opts)
	require.NotNil(t, err)
	require.Regexp(t, "fail to open MySQL connection.*ErrMySQLConnectionError.*", err)

	// test sink uri right
	sinkURI = "blackhole://"
	err = Validate(ctx, sinkURI, replicateConfig, opts)
	require.Nil(t, err)
}
