package metaclient

import "context"

// Client defines some basice method used as a meta client
type Client interface {
	// Close is the method to close the client and release inner resources
	Close() error

	// GenEpoch generate the increasing epoch for user
	GenEpoch(ctx context.Context) (int64, error)
}

// KVClient combines Client interface and KV interface
type KVClient interface {
	Client
	KV
}
