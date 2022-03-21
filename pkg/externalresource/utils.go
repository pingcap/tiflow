package externalresource

import (
	"context"

	"go.uber.org/atomic"

	"github.com/hanfei1991/microcosm/client"
	"github.com/hanfei1991/microcosm/pb"
)

// ignoreAfterSuccClient will ignore the invocations after the first success,
// to reduce unnecessary requests.
// Currently it only implements above behaviour for PersistResource. Other
// methods are same as MasterClient.
type ignoreAfterSuccClient struct {
	client.MasterClient
	success atomic.Bool
}

func (c *ignoreAfterSuccClient) PersistResource(
	ctx context.Context,
	request *pb.PersistResourceRequest,
) (*pb.PersistResourceResponse, error) {
	if c.success.Load() {
		return &pb.PersistResourceResponse{}, nil
	}
	resp, err := c.MasterClient.PersistResource(ctx, request)
	if err != nil || resp.Err != nil {
		return resp, err
	}
	c.success.Store(true)
	return resp, nil
}
