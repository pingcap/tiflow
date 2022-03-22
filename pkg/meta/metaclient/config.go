package metaclient

import "strings"

const (
	FrameMetaID       = "root"
	DefaultUserMetaID = "default"

	DefaultFrameMetaEndpoints = "127.0.0.1:12379"
	DefaultUserMetaEndpoints  = "127.0.0.1:12479"
)

type AuthConfParams struct {
	User   string `toml:"user" json:"user"`
	Passwd string `toml:"passwd" json:"passwd"`
}

type StoreConfigParams struct {
	// storeID is the unique readable identifier for a store
	StoreID   string         `toml:"store-id" json:"store-id"`
	Endpoints []string       `toml:"endpoints" json:"endpoints"`
	Auth      AuthConfParams `toml:"auth" json:"auth"`
}

func (s *StoreConfigParams) SetEndpoints(endpoints string) {
	if endpoints != "" {
		s.Endpoints = strings.Split(endpoints, ",")
	}
}
