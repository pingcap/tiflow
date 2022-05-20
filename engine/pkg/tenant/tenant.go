package tenant

type (
	// Tenant is the tenant id type
	Tenant = string
	// ProjectID is the project id type of tenant
	ProjectID = string
)

// tenant const variables
const (
	FrameTenantID       = "dfe_root"
	TestTenantID        = "dfe_test"
	DefaultUserTenantID = "def_default_user"
)

// ProjectInfo is the tenant/project information which is consistent with cloud service provider
type ProjectInfo struct {
	TenantID  Tenant
	ProjectID ProjectID
}
