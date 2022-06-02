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
var (
	FrameProjectInfo = ProjectInfo{
		tenantID:  "dfe_tenant_root",
		projectID: "dfe_proj_root",
	}
	TestProjectInfo = ProjectInfo{
		tenantID:  "dfe_tenant_test",
		projectID: "dfe_proj_test",
	}
	DefaultUserProjectInfo = ProjectInfo{
		tenantID:  "dfe_tenant_default",
		projectID: "dfe_proj_default",
	}
)

// NewProjectInfo return an immutable ProjectInfo
func NewProjectInfo(tenant string, project string) ProjectInfo {
	return ProjectInfo{
		tenantID:  tenant,
		projectID: project,
	}
}

// ProjectInfo is the tenant/project information which is consistent with cloud service provider
type ProjectInfo struct {
	tenantID  Tenant
	projectID ProjectID
}

// UniqueID get the unique id for project
// Theoretically, ProjectID is global uniqueness and can used as the identifier
// We offer this method here to hide the implementation of getting a unique ID
func (p ProjectInfo) UniqueID() string {
	return p.projectID
}

// TenantID return tenant id
func (p ProjectInfo) TenantID() string {
	return p.tenantID
}

// ProjectID return project id
func (p ProjectInfo) ProjectID() string {
	return p.projectID
}
