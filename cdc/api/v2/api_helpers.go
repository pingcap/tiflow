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

package v2

import (
	"context"

	tidbkv "github.com/pingcap/tidb/kv"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/owner"
	"github.com/pingcap/tiflow/pkg/security"
	pd "github.com/tikv/pd/client"
)

// APIV2Helpers is a collections of helper functions of OpenAPIV2.
// Defining it as an interface to make APIs more testable.
type APIV2Helpers interface {
	// VerifyCreateChangefeedConfig verifies the changefeedConfig,
	// and yield an valid changefeedInfo or error
	VerifyCreateChangefeedConfig(
		context.Context,
		*ChangefeedConfig,
		pd.Client,
		owner.StatusProvider,
		string,
		tidbkv.Storage,
	) (*model.ChangeFeedInfo, error)

	// VerifyUpdateChangefeedConfig verifies the changefeed update config,
	// and returns a pair of valid changefeedInfo & upstreamInfo
	VerifyUpdateChangefeedConfig(
		context.Context,
		*ChangefeedConfig,
		*model.ChangeFeedInfo,
		*model.UpstreamInfo,
	) (*model.ChangeFeedInfo, *model.UpstreamInfo, error)

	// VerifyUpstream verifies the upstreamConfig
	VerifyUpstream(
		context.Context,
		*ChangefeedConfig,
		*model.ChangeFeedInfo,
	) error

	// GetPDClient returns a PDClient given the PD cluster addresses and a credential
	GetPDClient(context.Context, []string, *security.Credential) (pd.Client, error)

	// CreateTiStore wrap the CreateTiStore method to increase testability
	CreateTiStore([]string, *security.Credential) (tidbkv.Storage, error)
}

// APIV2HelpersImpl is an implementation of AVIV2Helpers interface
type APIV2HelpersImpl struct{}
