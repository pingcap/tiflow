package cdc

import (
	"context"
	"net/http"
	"strings"

	"github.com/pingcap/log"
	"go.uber.org/zap"
)

type options struct {
	pdEndpoints string
	statusHost  string
	statusPort  int
}

var defaultServerOptions = options{
	pdEndpoints: "127.0.0.1:2379",
	statusHost:  "127.0.0.1",
	statusPort:  defaultStatusPort,
}

// PDEndpoints returns a ServerOption that sets the endpoints of PD for the server.
func PDEndpoints(s string) ServerOption {
	return func(o *options) {
		o.pdEndpoints = s
	}
}

// StatusHost returns a ServerOption that sets the status server host
func StatusHost(s string) ServerOption {
	return func(o *options) {
		o.statusHost = s
	}
}

// StatusPort returns a ServerOption that sets the status server port
func StatusPort(p int) ServerOption {
	return func(o *options) {
		o.statusPort = p
	}
}

// A ServerOption sets options such as the addr of PD.
type ServerOption func(*options)

// Server is the capture server
type Server struct {
	opts         options
	capture      *Capture
	statusServer *http.Server
}

// NewServer creates a Server instance.
func NewServer(opt ...ServerOption) (*Server, error) {
	opts := defaultServerOptions
	for _, o := range opt {
		o(&opts)
	}
	log.Info("creating CDC server",
		zap.String("pd-addr", opts.pdEndpoints),
		zap.String("status-host", opts.statusHost),
		zap.Int("status-port", opts.statusPort))

	capture, err := NewCapture(strings.Split(opts.pdEndpoints, ","))
	if err != nil {
		return nil, err
	}

	s := &Server{
		opts:    opts,
		capture: capture,
	}
	return s, nil
}

// Run runs the server.
func (s *Server) Run(ctx context.Context) error {
	s.startStatusHTTP()
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
	if s.statusServer != nil {
		err := s.statusServer.Close()
		if err != nil {
			log.Error("close status server", zap.Error(err))
		}
		s.statusServer = nil
	}
	cancel()
}
