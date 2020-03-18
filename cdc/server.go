// Copyright 2019 PingCAP, Inc.
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

package cdc

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/util"
	"go.uber.org/zap"
)

// Config holds config for cdc server
type Config struct {
	Security   *util.Security `json:"security"`
	PD         string         `json:"pd"`
	StatusAddr string         `json:"status-addr"`
}

// String implements fmt.Stringer
func (c *Config) String() string {
	data, err := json.Marshal(c)
	if err != nil {
		log.Warn("marshal config error", zap.Error(err), zap.Reflect("config", c))
	}
	return string(data)
}

// Server is the capture server
type Server struct {
	config       *Config
	capture      *Capture
	statusServer *http.Server
}

// NewServer creates a Server instance.
func NewServer(cfg *Config) (*Server, error) {
	log.Info("creating CDC server", zap.Stringer("config", cfg))

	capture, err := NewCapture(strings.Split(cfg.PD, ","), cfg.Security)
	if err != nil {
		return nil, err
	}

	s := &Server{
		config:  cfg,
		capture: capture,
	}
	return s, nil
}

// Run runs the server.
func (s *Server) Run(ctx context.Context) error {
	s.startStatusHTTP()
	ctx = util.PutCaptureIDInCtx(ctx, s.capture.info.ID)
	return s.capture.Start(ctx)
}

// Close closes the server.
func (s *Server) Close() {
	if s.statusServer != nil {
		err := s.statusServer.Close()
		if err != nil {
			log.Error("close status server", zap.Error(err))
		}
		s.statusServer = nil
	}
	if s.capture != nil {
		s.capture.Cleanup()

		closeCtx, closeCancel := context.WithTimeout(context.Background(), time.Second*2)
		err := s.capture.Close(closeCtx)
		if err != nil {
			log.Error("close capture", zap.Error(err))
		}
		closeCancel()
	}
}
