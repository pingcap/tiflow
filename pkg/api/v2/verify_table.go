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

	v2 "github.com/pingcap/tiflow/cdc/api/v2"
	"github.com/pingcap/tiflow/pkg/api/internal/rest"
)

// VerifyTableGetter has a method to return a VerifyTableInterface.
type VerifyTableGetter interface {
	VerifyTable() VerifyTableInterface
}

// VerifyTableInterface has methods to work with verifyTable api
type VerifyTableInterface interface {
	Verify(ctx context.Context, verifyTableConfig *v2.VerifyTableConfig) (*v2.Tables, error)
}

// verifyTable implements VerifyTableInterface
type verifyTable struct {
	client rest.CDCRESTInterface
}

// newUnsafe returns unsafe
func newVerifyTable(c *APIV2Client) *verifyTable {
	return &verifyTable{
		client: c.RESTClient(),
	}
}

// VerifyTable returns ...
func (c *verifyTable) Verify(ctx context.Context,
	verifyTableConfig *v2.VerifyTableConfig,
) (*v2.Tables, error) {
	result := new(v2.Tables)
	err := c.client.Post().
		WithURI("verify-table").
		WithBody(verifyTableConfig).
		Do(ctx).
		Into(result)
	return result, err
}
