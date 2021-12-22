// Copyright 2020 PingCAP, Inc.
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

package capture

import (
	"context"
	"io"
	"net"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"go.uber.org/zap"
)

const ownershipCheckDuration = 1 * time.Second

// startStatusServer is a thread safe method
func (s *Server) startStatusServer() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closeStatusServer()
	// discard gin log output
	gin.DefaultWriter = io.Discard
	var router *gin.Engine
	if s.capture.IsOwner() {
		router = NewRouter(NewHTTPHandler(s.capture, s.capture.owner.StatusProvider))
	} else {
		router = NewRouter(NewHTTPHandler(s.capture, nil))
	}

	conf := config.GetGlobalServerConfig()
	// if CertAllowedCN was specified, we should add server's common name
	// otherwise, https requests sent to non-owner capture can't be forward
	// to owner
	if len(conf.Security.CertAllowedCN) != 0 {
		err := conf.Security.AddSelfCommonName()
		if err != nil {
			log.Error("status server set tls config failed", zap.Error(err))
			return errors.Trace(err)
		}
	}

	tlsConfig, err := conf.Security.ToTLSConfigWithVerify()
	if err != nil {
		log.Error("status server get tls config failed", zap.Error(err))
		return errors.Trace(err)
	}

	s.statusServer = &http.Server{Addr: conf.Addr, Handler: router, TLSConfig: tlsConfig}

	ln, err := net.Listen("tcp", conf.Addr)
	if err != nil {
		return cerror.WrapError(cerror.ErrServeHTTP, err)
	}
	go func() {
		log.Info("status server is running", zap.String("addr", conf.Addr))
		if tlsConfig != nil {
			err = s.statusServer.ServeTLS(ln, conf.Security.CertPath, conf.Security.KeyPath)
		} else {
			err = s.statusServer.Serve(ln)
		}
		if err != nil && err != http.ErrServerClosed {
			log.Error("status server error", zap.Error(cerror.WrapError(cerror.ErrServeHTTP, err)))
		}
	}()
	return nil
}

// runStatusServer runs and maintains cdc status server, when capture ownership changed
// it will update the inner handler of the status server
func (s *Server) runStatusServer(ctx context.Context) error {
	ticker := time.NewTicker(ownershipCheckDuration)
	defer ticker.Stop()

	ownership := s.capture.IsOwner()
	err := s.startStatusServer()
	if err != nil {
		return err
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if ownership != s.capture.IsOwner() {
				ownership = s.capture.IsOwner()
				s.updateServerHandler()
				log.Info("status server reset due to capture's ownership changed")
			}
		}
	}
}

func (s *Server) updateServerHandler() {
	var router *gin.Engine
	if s.capture.IsOwner() {
		s.capture.ownerMu.Lock()
		router = NewRouter(NewHTTPHandler(s.capture, s.capture.owner.StatusProvider))
		s.capture.ownerMu.Unlock()
	} else {
		router = NewRouter(NewHTTPHandler(s.capture, nil))
	}
	s.mu.Lock()
	s.statusServer.Handler = router
	s.mu.Unlock()
}

// closeStatusServer is non-thread safe, the caller must acquire s.mu
func (s *Server) closeStatusServer() {
	if s.statusServer != nil {
		err := s.statusServer.Close()
		if err != nil {
			log.Error("close status server", zap.Error(err), zap.Stack("stack"))
		}
		s.statusServer = nil
	}
}
