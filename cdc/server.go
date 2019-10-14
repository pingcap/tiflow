package cdc

import (
	"strings"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/pingcap/errors"
	"google.golang.org/grpc"
)

type options struct {
	pdEndpoints string
}

var defaultServerOptions = options{
	pdEndpoints: "127.0.0.1:2379",
}

// PDEndpoints returns a ServerOption that sets the endpoints of PD for the server.
func PDEndpoints(s string) ServerOption {
	return func(o *options) {
		o.pdEndpoints = s
	}
}

// A ServerOption sets options such as the addr of PD.
type ServerOption func(*options)

// Server is the capture server
type Server struct {
	opts options
	id   string

	etcdClient *clientv3.Client
}

// NewServer creates a Server instance.
func NewServer(opt ...ServerOption) (*Server, error) {
	opts := defaultServerOptions
	for _, o := range opt {
		o(&opts)
	}

	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   strings.Split(opts.pdEndpoints, ","),
		DialTimeout: 5 * time.Second,
		DialOptions: []grpc.DialOption{
			grpc.WithBackoffMaxDelay(time.Second * 3),
		},
	})

	if err != nil {
		return nil, errors.Annotate(err, "new etcd client")
	}

	s := &Server{
		opts:       opts,
		etcdClient: cli,
	}

	return s, nil
}

// Run runs the server.
func (s *Server) Run() error {

	return nil
}

// Close closes the server.
func (s *Server) Close() {

}
