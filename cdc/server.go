package cdc

import (
	"context"
	"strings"
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
func (s *Server) Run() error {
	capture, err := NewCapture(strings.Split(s.opts.pdEndpoints, ","))
	if err != nil {
		return err
	}
	return capture.Start(context.Background())
}

// Close closes the server.
func (s *Server) Close() {

}
