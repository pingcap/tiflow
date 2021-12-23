package p2p

import (
	"context"
	"net"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/dm/pkg/log"
	p2pImpl "github.com/pingcap/tiflow/pkg/p2p"
	"github.com/pingcap/tiflow/pkg/security"
	"github.com/pingcap/tiflow/proto/p2p"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
)

// Re-export some types
type (
	Topic  = p2pImpl.Topic
	NodeID = p2pImpl.NodeID
	Config = p2pImpl.MessageServerConfig
)

type (
	TypeInformation = interface{}
	MessageValue    = interface{}
	HandlerFunc     = func(sender NodeID, value MessageValue) error
)

type (
	MessageServerOpt = func(*Config)
)

// read only
var defaultServerConfig = Config{
	MaxPendingMessageCountPerTopic:       256,
	MaxPendingTaskCount:                  102400,
	SendChannelSize:                      16,
	AckInterval:                          time.Millisecond * 200,
	WorkerPoolSize:                       4,
	MaxPeerCount:                         1024,
	WaitUnregisterHandleTimeoutThreshold: time.Millisecond * 100,
}

// MessageRPCService is a background service wrapping a MessageServer instance.
type MessageRPCService struct {
	messageServer *p2pImpl.MessageServer
	grpcServer    *grpc.Server
}

// NewMessageRPCService creates a new MessageRPCService.
// Note: TLS is not supported for now.
func NewMessageRPCService(
	selfID NodeID,
	_credential *security.Credential,
	opts ...MessageServerOpt,
) (*MessageRPCService, error) {
	// Deep copy
	config := defaultServerConfig
	// Apply opts
	for _, opt := range opts {
		opt(&config)
	}

	messageServer := p2pImpl.NewMessageServer(selfID, &config)
	// TODO zixiong: support accepting TLS connections.
	grpcSvr := grpc.NewServer()
	p2p.RegisterCDCPeerToPeerServer(grpcSvr, messageServer)
	return &MessageRPCService{
		messageServer: messageServer,
		grpcServer:    grpcSvr,
	}, nil
}

// Serve listens on `l` and creates the background goroutine for the message server.
func (s *MessageRPCService) Serve(ctx context.Context, l net.Listener) error {
	defer func() {
		err := l.Close()
		if err != nil {
			log.L().Warn("failed to close Listener", zap.Error(err))
		}
	}()

	wg, ctx := errgroup.WithContext(ctx)

	wg.Go(func() (err error) {
		defer log.L().ErrorFilterContextCanceled("message server exited", zap.Error(err))
		return errors.Trace(s.messageServer.Run(ctx))
	})

	wg.Go(func() (err error) {
		defer func() {
			// TODO (zixiong) filter out expected harmless errors.
			log.L().Debug("grpc server exited", zap.Error(err))
		}()
		return errors.Trace(s.grpcServer.Serve(l))
	})

	// We need a separate goroutine for canceling the gRPC server
	// because the `Serve` method provides by the library does not
	// support canceling by contexts, which is a more idiomatic way.
	wg.Go(func() error {
		<-ctx.Done()
		log.L().Debug("context canceled, stopping the gRPC server")

		s.grpcServer.Stop()
		return nil
	})

	return wg.Wait()
}

// MakeHandlerManager returns a MessageHandlerManager
func (s *MessageRPCService) MakeHandlerManager() MessageHandlerManager {
	return newMessageHandlerManager(s.messageServer)
}
