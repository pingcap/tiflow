package externalresource

import (
	"context"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/hanfei1991/microcosm/client"
	"github.com/hanfei1991/microcosm/pb"
)

func TestIgnoreAfterSuccClient(t *testing.T) {
	mockCli := &client.MockServerMasterClient{}
	c := &ignoreAfterSuccClient{MasterClient: mockCli}

	ctx := context.Background()
	req := &pb.PersistResourceRequest{}

	// mock expect once
	mockCli.Mock.On("PersistResource", mock.Anything, mock.Anything).
		Return(&pb.PersistResourceResponse{}, nil)

	// call twice
	check := func(resp *pb.PersistResourceResponse, err error) {
		require.NoError(t, err)
		require.Nil(t, resp.Err)
	}
	check(c.PersistResource(ctx, req))
	check(c.PersistResource(ctx, req))
}
