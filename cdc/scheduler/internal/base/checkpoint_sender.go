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

package base

import (
	"context"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/cdc/model"
	"go.uber.org/zap"
)

type checkpointProviderFunc = func() (checkpointTs, resolvedTs model.Ts, ok bool)

// checkpointSender is used to send checkpoints (as well as other watermarks)
// to the owner's scheduler.
// It DOES NOT need to be thread-safe.
type checkpointSender interface {
	// SendCheckpoint sends a checkpoint to the owner's scheduler.
	// provider is called to get the latest checkpoint when needed.
	SendCheckpoint(ctx context.Context, provider checkpointProviderFunc) error
	// LastSentCheckpointTs returns the last checkpoint sent to the owner's scheduler.
	LastSentCheckpointTs() model.Ts
}

const (
	noEnRouteCheckpointTs = 0
)

type checkpointTsSenderImpl struct {
	communicator ProcessorMessenger
	logger       *zap.Logger

	// We use a `clock.Clock` here to make time mockable in unit tests.
	clock clock.Clock

	// The wall-clock time when we sent the last checkpoint-ts.
	lastSendCheckpointTime time.Time

	// The last checkpoint-ts guaranteed to have been received.
	lastSentCheckpoint model.Ts
	// The last checkpoint-ts already sent
	// but not yet guaranteed to have been received.
	enRouteCheckpointTs model.Ts

	// config parameter. Read only.
	sendCheckpointTsInterval time.Duration
}

func newCheckpointSender(
	communicator ProcessorMessenger,
	logger *zap.Logger,
	sendCheckpointTsInterval time.Duration,
) checkpointSender {
	return &checkpointTsSenderImpl{
		communicator:             communicator,
		logger:                   logger,
		clock:                    clock.New(),
		sendCheckpointTsInterval: sendCheckpointTsInterval,
	}
}

// SendCheckpoint sends a checkpoint to the owner's scheduler.
func (s *checkpointTsSenderImpl) SendCheckpoint(ctx context.Context, provider checkpointProviderFunc) error {
	// First check if there is a checkpoint en route to the Owner.
	if s.enRouteCheckpointTs != noEnRouteCheckpointTs {
		if !s.communicator.Barrier(ctx) {
			s.logger.Debug("not sending checkpoint due to pending barrier",
				zap.Duration("sinceLastSent", time.Since(s.lastSendCheckpointTime)))
			// We cannot proceed because the last checkpoint has not been acknowledged to have
			// been received.
			return nil
		}
		// The last checkpoint HAS been acknowledged.
		s.lastSentCheckpoint, s.enRouteCheckpointTs = s.enRouteCheckpointTs, noEnRouteCheckpointTs
	}

	checkpointTs, resolvedTs, ok := provider()
	if !ok {
		s.logger.Debug("no checkpoint to send")
		return nil
	}

	if s.clock.Since(s.lastSendCheckpointTime) < s.sendCheckpointTsInterval {
		return nil
	}

	done, err := s.communicator.SendCheckpoint(ctx, checkpointTs, resolvedTs)
	if err != nil {
		return errors.Trace(err)
	}
	if done {
		s.enRouteCheckpointTs = checkpointTs
		s.lastSendCheckpointTime = s.clock.Now()
	}
	return nil
}

// LastSentCheckpointTs returns the last checkpoint sent to the owner's scheduler.
func (s *checkpointTsSenderImpl) LastSentCheckpointTs() model.Ts {
	return s.lastSentCheckpoint
}
