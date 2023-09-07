// Copyright 2022 PingCAP, Inc.
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

package election

import (
	"context"
	"time"

	"github.com/pingcap/tiflow/pkg/errors"
)

const (
	defaultLeaseDuration  = time.Second * 10
	defaultRenewInterval  = time.Second * 2
	defaultRenewDeadline  = defaultLeaseDuration - defaultRenewInterval
	defaultReleaseTimeout = 5 * time.Second
	defaultResignTimeout  = 5 * time.Second
)

// Config is the configuration for the leader election.
type Config struct {
	// ID is id of the current member.
	// It is used to distinguish different members.
	// It must be unique among all members.
	ID string
	// Name is the human-readable name of the current member.
	// It is used for logging and diagnostics.
	Name string
	// Address is the address of the current member.
	Address string
	// Storage is the storage to store the election record.
	Storage Storage
	// LeaderCallback is the callback function when the current member becomes leader.
	// If current member loses the leadership, the ctx will be canceled. Callback should
	// return as soon as possible when the ctx is canceled.
	LeaderCallback func(ctx context.Context) error
	// LeaseDuration is the duration that a client will wait before
	// it can remove this member when the client hasn't observed any
	// renewals from the member.
	//
	// A client will wait a full LeaseDuration without observing any
	// renewals from the member before it can remove the member.
	// When all clients are shutdown and a new set of clients are started
	// with different id against the same storage, they must wait a full
	// LeaseDuration before they can acquire a leadership. So the LeaseDuration
	// should be set as short as possible to avoid the long waits in this case.
	//
	// It defaults to 10 seconds if not set.
	LeaseDuration time.Duration
	// RenewInterval is the interval that the current member tries to
	// refresh the election record and renew the lease.
	//
	// It defaults to 2 seconds if not set.
	RenewInterval time.Duration
	// RenewDeadline is the duration that the current member waits before
	// it gives up on the leadership when it can't renew the lease. To avoid
	// two members from both acting as leader at the same time, RenewDeadline
	// should be set shorter than LeaseDuration - RenewInterval.
	//
	// It defaults to 8 seconds if not set.
	RenewDeadline time.Duration
	// ExitOnRenewFail indicates whether the current member should exit when
	// failing to renew the lease.
	ExitOnRenewFail bool
}

// AdjustAndValidate adjusts the config and validates it.
func (c *Config) AdjustAndValidate() error {
	if c.LeaseDuration == 0 {
		c.LeaseDuration = defaultLeaseDuration
	}
	if c.RenewInterval == 0 {
		c.RenewInterval = defaultRenewInterval
	}
	if c.RenewDeadline == 0 {
		c.RenewDeadline = defaultRenewDeadline
	}
	if c.ID == "" {
		return errors.Errorf("id must not be empty")
	}
	if c.Storage == nil {
		return errors.Errorf("storage must not be nil")
	}
	if c.LeaderCallback == nil {
		return errors.Errorf("LeaderCallback must not be nil")
	}
	if c.RenewInterval > c.LeaseDuration {
		return errors.Errorf("RenewInterval must not be greater than LeaseDuration")
	}
	if c.RenewDeadline > c.LeaseDuration-c.RenewInterval {
		return errors.Errorf("RenewDeadline must not be greater than LeaseDuration - RenewInterval")
	}
	return nil
}
