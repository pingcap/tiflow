package cdc

import (
	"context"
	"strings"

	"github.com/pingcap/log"
	"go.uber.org/zap"
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
	opts    options
	capture *Capture
}

// NewServer creates a Server instance.
func NewServer(opt ...ServerOption) (*Server, error) {
	opts := defaultServerOptions
	for _, o := range opt {
		o(&opts)
	}

	s := &Server{
		opts: opts,
	}

	return s, nil
}

// Run runs the server.
func (s *Server) Run(ctx context.Context) error {
	capture, err := NewCapture(strings.Split(s.opts.pdEndpoints, ","))
	if err != nil {
		return err
	}
	s.capture = capture
	return s.capture.Start(ctx)
}

// Close closes the server.
func (s *Server) Close(ctx context.Context, cancel context.CancelFunc) {
	if s.capture != nil {
		err := s.capture.Close(ctx)
		if err != nil {
			log.Error("close capture", zap.Error(err))
		}
	}
	cancel()
}
