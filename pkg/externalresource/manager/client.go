package manager

import (
	"context"
	"time"

	"github.com/hanfei1991/microcosm/pb"
	"github.com/hanfei1991/microcosm/pkg/errors"
	"github.com/hanfei1991/microcosm/pkg/rpcutil"
	"google.golang.org/grpc"
)

const dialTimeout = 5 * time.Second

var dialImpl = func(ctx context.Context, addr string) (pb.ResourceManagerClient, rpcutil.CloseableConnIface, error) {
	ctx, cancel := context.WithTimeout(ctx, dialTimeout)
	defer cancel()
	conn, err := grpc.DialContext(ctx, addr, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		return nil, nil, errors.Wrap(errors.ErrGrpcBuildConn, err)
	}
	return pb.NewResourceManagerClient(conn), conn, nil
}

// NewResourceClient creates a new resource manager rpc client
func NewResourceClient(ctx context.Context, join []string,
) (*rpcutil.FailoverRPCClients[pb.ResourceManagerClient], error) {
	clients, err := rpcutil.NewFailoverRPCClients(ctx, join, dialImpl)
	if err != nil {
		return nil, err
	}
	return clients, nil
}
