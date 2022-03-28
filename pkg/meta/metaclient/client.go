package metaclient

import "context"

type ClientType int

const (
	TypeKVClient ClientType = iota /* KV client style，like etcd/consul/TiKV/redis or even SQL backend*/
)

type Client interface {
	// Close is the method to close the client and release inner resources
	Close() error

	// GenEpoch generate the increasing epoch for user
	GenEpoch(ctx context.Context) (int64, error)
}

type KVClient interface {
	Client
	KV
}
