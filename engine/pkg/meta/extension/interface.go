package extension

import (
	"context"

	"github.com/hanfei1991/microcosm/pkg/meta/metaclient"
)

// KVEx extends the KV interface with Do method to implement the intermediate
// layer easier
type KVEx interface {
	metaclient.KV

	// Do applies a single Op on KV without a transaction.
	// Do is useful when adding intermidate layer to KV implement
	Do(ctx context.Context, op metaclient.Op) (metaclient.OpResponse, metaclient.Error)
}

// KVClientEx extends the KVClient interface with Do method to implement the
// intermediate layer easier
type KVClientEx interface {
	KVEx
	metaclient.Client
}
