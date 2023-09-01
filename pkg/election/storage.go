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
)

// Storage describes the requirements of a storage that can be used for leader election.
type Storage interface {
	// Get gets the current record from the storage. It returns the current record stored
	// in the storage and any error encountered. Note that if the record has not created yet,
	// it should return an empty record and a nil error.
	Get(ctx context.Context) (*Record, error)
	// Update updates the record in the storage if the stored record version matches the given
	// record version. It returns ErrRecordConflict if the stored record version does not match
	// or any other error encountered.
	Update(ctx context.Context, record *Record) error
}

// Record holds the information of a leader election.
type Record struct {
	// LeaderID is the ID of the leader. If it is empty, it means
	// there is no leader and all members may try to become leader.
	LeaderID string `json:"leader_id"`
	// Members is the members that are eligible to become leader,
	// which includes the leader itself.
	Members []*Member `json:"members"`
	// Version is the internal version of the record. It can be used by
	// specific storage implementation to determine whether the record has
	// been changed by another writer. The caller of the storage interface
	// should treat this value as opaque.
	//
	// Skip serializing this field, because it is not part of the record content.
	Version int64 `json:"-"`
}

// Member is a member in a leader election.
type Member struct {
	// ID is the ID of the member.
	ID string `json:"id"`
	// Name is the human-readable name of the member.
	// It is used for logging and diagnostics.
	Name string `json:"name"`
	// Address is the address of the member.
	Address string `json:"address"`
	// LeaseDuration is the duration that a client will wait before
	// it can remove this member when the client hasn't observed
	// any renewals from the member.
	LeaseDuration time.Duration `json:"lease_duration"`
	// RenewTime is the last time when the member renewed its lease.
	RenewTime time.Time `json:"renew_time"`
}

// Clone returns a deep copy of the member.
func (m *Member) Clone() *Member {
	return &Member{
		ID:            m.ID,
		Name:          m.Name,
		Address:       m.Address,
		LeaseDuration: m.LeaseDuration,
		RenewTime:     m.RenewTime,
	}
}

// Clone returns a deep copy of the record.
func (r *Record) Clone() *Record {
	members := make([]*Member, len(r.Members))
	for i, m := range r.Members {
		members[i] = m.Clone()
	}
	return &Record{
		LeaderID: r.LeaderID,
		Members:  members,
		Version:  r.Version,
	}
}

// FindMember finds the member with the given ID in the record.
func (r *Record) FindMember(id string) (*Member, bool) {
	for _, m := range r.Members {
		if m.ID == id {
			return m, true
		}
	}
	return nil, false
}
