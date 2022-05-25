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

package tenant

type (
	// Tenant is the tenant id type
	Tenant = string
	// ProjectID is the project id type of tenant
	ProjectID = string
)

// tenant const variables
const (
	FrameTenantID        = "dfe_tenant_root"
	FrameProjectID       = "dfe_proj_root"
	TestTenantID         = "dfe_tenant_test"
	TestProjectID        = "dfe_proj_test"
	DefaultUserTenantID  = "dfe_tenant_default"
	DefaultUserProjectID = "dfe_proj_default"
)

// ProjectInfo is the tenant/project information which is consistent with cloud service provider
type ProjectInfo struct {
	TenantID  Tenant
	ProjectID ProjectID
}
