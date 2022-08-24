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

package metadata

import (
	"context"

	"github.com/coreos/go-semver/semver"
	frameModel "github.com/pingcap/tiflow/engine/framework/model"
	"github.com/pingcap/tiflow/engine/pkg/adapter"
	metaModel "github.com/pingcap/tiflow/engine/pkg/meta/model"
)

// Info represents the cluster info.
type Info struct {
	State

	Version semver.Version
}

// NewInfo creates a new Info instance.
func NewInfo(version semver.Version) *Info {
	return &Info{
		Version: version,
	}
}

// InfoStore manages the state of info.
type InfoStore struct {
	*TomlStore

	id frameModel.MasterID
}

// NewInfoStore returns a new InfoStore instance
func NewInfoStore(id frameModel.MasterID, kvClient metaModel.KVClient) *InfoStore {
	infoStore := &InfoStore{
		TomlStore: NewTomlStore(kvClient),
		id:        id,
	}
	infoStore.TomlStore.Store = infoStore
	return infoStore
}

// CreateState creates an empty Info object
func (infoStore *InfoStore) CreateState() State {
	return &Info{}
}

// Key returns encoded key of info state store
func (infoStore *InfoStore) Key() string {
	return adapter.DMInfoKeyAdapter.Encode(infoStore.id)
}

// UpdateVersion updates the version of info.
func (infoStore *InfoStore) UpdateVersion(ctx context.Context, newVer semver.Version) error {
	state, err := infoStore.Get(ctx)
	if err != nil {
		return err
	}
	info := state.(*Info)
	info.Version = newVer
	return infoStore.Put(ctx, info)
}
