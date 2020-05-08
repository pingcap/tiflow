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
	"net/http"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/util"
	"go.etcd.io/etcd/mvcc"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

const (
	ownerRunInterval = time.Millisecond * 500

	// DefaultCDCGCSafePointTTL is the default value of cdc gc safe-point ttl, specified in seconds.
	DefaultCDCGCSafePointTTL = 24 * 60 * 60
)

type options struct {
	pdEndpoints string
	statusHost  string
	statusPort  int
	gcTTL       int64
	timezone    *time.Location
}

var defaultServerOptions = options{
	pdEndpoints: "http://127.0.0.1:2379",
	statusHost:  "127.0.0.1",
	statusPort:  defaultStatusPort,
	timezone:    nil,
	gcTTL:       DefaultCDCGCSafePointTTL,
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

// GCTTL returns a ServerOption that sets the gc ttl.
func GCTTL(t int64) ServerOption {
	return func(o *options) {
		o.gcTTL = t
	}
}

// Timezone returns a ServerOption that sets the timezone
func Timezone(tz *time.Location) ServerOption {
	return func(o *options) {
		o.timezone = tz
	}
}

// A ServerOption sets options such as the addr of PD.
type ServerOption func(*options)

// Server is the capture server
type Server struct {
	opts         options
	capture      *Capture
	owner        *Owner
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
		zap.Int("status-port", opts.statusPort),
		zap.Int64("gc-ttl", opts.gcTTL),
		zap.Any("timezone", opts.timezone))

	s := &Server{
		opts: opts,
	}
	return s, nil
}

// Run runs the server.
func (s *Server) Run(ctx context.Context) error {
	s.startStatusHTTP()

	// When a capture suicided, restart it
	for {
		if err := s.run(ctx); errors.Cause(err) != ErrSuicide {
			return err
		}
		log.Info("server recovered", zap.String("capture", s.capture.info.ID))
	}
}

func (s *Server) campaignOwnerLoop(ctx context.Context) error {
	// In most failure cases, we don't return error directly, just run another
	// campaign loop. We treat campaign loop as a special background routine.
	for {
		// Campaign to be an owner, it blocks until it becomes the owner
		if err := s.capture.Campaign(ctx); err != nil {
			switch errors.Cause(err) {
			case context.Canceled:
				return nil
			case mvcc.ErrCompacted:
				continue
			}
			log.Warn("campaign owner failed", zap.Error(err))
			continue
		}
		log.Info("campaign owner successfully", zap.String("capture", s.capture.info.ID))
		owner, err := NewOwner(s.capture.session, s.opts.gcTTL)
		if err != nil {
			log.Warn("create new owner failed", zap.Error(err))
			continue
		}

		s.owner = owner
		if err := owner.Run(ctx, ownerRunInterval); err != nil {
			if errors.Cause(err) == context.Canceled {
				log.Info("owner exited", zap.String("capture", s.capture.info.ID))
				return nil
			}
			err2 := s.capture.Resign(ctx)
			if err2 != nil {
				// if regisn owner failed, return error to let capture exits
				return errors.Annotatef(err2, "resign owner failed, capture: %s", s.capture.info.ID)
			}
			log.Warn("run owner failed", zap.Error(err))
		}
		// owner is resigned by API, reset owner and continue the campaign loop
		s.owner = nil
	}
}

func (s *Server) run(ctx context.Context) (err error) {
	capture, err := NewCapture(strings.Split(s.opts.pdEndpoints, ","))
	if err != nil {
		return err
	}
	s.capture = capture
	ctx = util.PutCaptureIDInCtx(ctx, s.capture.info.ID)
	ctx = util.PutTimezoneInCtx(ctx, s.opts.timezone)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	wg, cctx := errgroup.WithContext(ctx)

	wg.Go(func() error {
		return s.campaignOwnerLoop(cctx)
	})

	wg.Go(func() error {
		return s.capture.Run(cctx)
	})

	return wg.Wait()
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
