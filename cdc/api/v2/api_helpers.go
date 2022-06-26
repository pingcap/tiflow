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
	// verifyCreateChangefeedConfig verifies the changefeedConfig,
	// and yield an valid changefeedInfo or error
	verifyCreateChangefeedConfig(
		context.Context,
		*ChangefeedConfig,
		pd.Client,
		owner.StatusProvider,
		string,
		tidbkv.Storage,
	) (*model.ChangeFeedInfo, error)

	// verifyUpdateChangefeedConfig verifies the changefeed update config,
	// and returns a pair of valid changefeedInfo & upstreamInfo
	verifyUpdateChangefeedConfig(
		context.Context,
		*ChangefeedConfig,
		*model.ChangeFeedInfo,
		*model.UpstreamInfo,
	) (*model.ChangeFeedInfo, *model.UpstreamInfo, error)

	// verifyUpstream verifies the upstreamConfig
	verifyUpstream(
		context.Context,
		*ChangefeedConfig,
		*model.ChangeFeedInfo,
	) error

	// getPDClient returns a PDClient given the PD cluster addresses and a credential
	getPDClient(context.Context, []string, *security.Credential) (pd.Client, error)

	// createTiStore wrap the createTiStore method to increase testability
	createTiStore([]string, *security.Credential) (tidbkv.Storage, error)
}

// APIV2HelpersImpl is an implementation of AVIV2Helpers interface
type APIV2HelpersImpl struct{}
