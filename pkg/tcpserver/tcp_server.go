// Copyright 2021 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package tcpserver

import (
	"context"
	"crypto/tls"
	"net"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/security"
	"github.com/soheilhy/cmux"
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

var cmuxReadTimeout = 10 * time.Second

// TCPServer provides a muxed socket that can
// serve both plain HTTP and gRPC at the same time.
type TCPServer interface {
	// Run runs the TCPServer.
	// For a given instance of TCPServer, Run is expected
	// to be called only once.
	Run(ctx context.Context) error
	// GrpcListener returns the gRPC listener that
	// can be listened on by a gRPC server.
	GrpcListener() net.Listener
	// HTTP1Listener returns a plain HTTP listener.
	HTTP1Listener() net.Listener
	// IsTLSEnabled returns whether TLS has been enabled.
	IsTLSEnabled() bool
	// Close closed the TCPServer.
	// The listeners returned by GrpcListener and HTTP1Listener
	// will be closed, which will force the consumers of these
	// listeners to stop. This provides a reliable mechanism to
	// cancel all related components.
	Close() error
}

type tcpServerImpl struct {
	mux cmux.CMux

	rootListener net.Listener
	// grpc lisener, service as p2p gRPC server.
	grpcListener net.Listener
	// used for HTTP server, service for restful open API.
	http1Listener net.Listener

	isClosed atomic.Bool

	credentials  *security.Credential
	isTLSEnabled bool // read only
}

// NewTCPServer creates a new TCPServer
func NewTCPServer(address string, credentials *security.Credential) (TCPServer, error) {
	lis, err := net.Listen("tcp", address)
	if err != nil {
		return nil, errors.Trace(err)
	}

	server := &tcpServerImpl{
		credentials: credentials,
	}

	if credentials.IsTLSEnabled() {
		tlsLis, err := wrapTLSListener(lis, credentials)
		if err != nil {
			return nil, errors.Trace(err)
		}
		server.rootListener = tlsLis
		server.isTLSEnabled = true
	} else {
		server.rootListener = lis
	}

	server.mux = cmux.New(server.rootListener)
	// We must set a read timeout for cmux, otherwise irresponsive clients
	// may block the server from exiting.
	// ref: https://github.com/pingcap/tidb-binlog/pull/352
	server.mux.SetReadTimeout(cmuxReadTimeout)

	server.grpcListener = server.mux.MatchWithWriters(
		cmux.HTTP2MatchHeaderFieldSendSettings("content-type", "application/grpc"))
	server.http1Listener = server.mux.Match(cmux.HTTP1Fast(), cmux.HTTP2())

	return server, nil
}

// Run runs the mux. The mux has to be running to accept connections.
func (s *tcpServerImpl) Run(ctx context.Context) error {
	if s.isClosed.Load() {
		return cerror.ErrTCPServerClosed.GenWithStackByArgs()
	}

	defer func() {
		s.isClosed.Store(true)
		// Closing the rootListener provides a reliable way
		// for telling downstream components to exit.
		_ = s.rootListener.Close()
	}()
	errg, ctx := errgroup.WithContext(ctx)

	errg.Go(func() error {
		err := s.mux.Serve()
		if err == cmux.ErrServerClosed {
			return cerror.ErrTCPServerClosed.GenWithStackByArgs()
		}
		if err != nil && strings.Contains(err.Error(), "use of closed network connection") {
			return cerror.ErrTCPServerClosed.GenWithStackByArgs()
		}
		return errors.Trace(err)
	})

	errg.Go(func() error {
		<-ctx.Done()
		log.Debug("cmux has been canceled", zap.Error(ctx.Err()))
		s.mux.Close()
		return nil
	})

	return errg.Wait()
}

func (s *tcpServerImpl) GrpcListener() net.Listener {
	return s.grpcListener
}

func (s *tcpServerImpl) HTTP1Listener() net.Listener {
	return s.http1Listener
}

func (s *tcpServerImpl) IsTLSEnabled() bool {
	return s.isTLSEnabled
}

func (s *tcpServerImpl) Close() error {
	if s.isClosed.Swap(true) {
		// ignore double closing
		return nil
	}
	// Closing the rootListener provides a reliable way
	// for telling downstream components to exit.
	return errors.Trace(s.rootListener.Close())
}

// wrapTLSListener takes a plain Listener and security credentials,
// and returns a listener that handles TLS connections.
func wrapTLSListener(inner net.Listener, credentials *security.Credential) (net.Listener, error) {
	config, err := credentials.ToTLSConfigWithVerify()
	if err != nil {
		return nil, errors.Trace(err)
	}
	// This is a hack to make `ToTLSConfigWithVerify` work with cmux,
	// since cmux does not support ALPN.
	config.NextProtos = nil

	return tls.NewListener(inner, config), nil
}
