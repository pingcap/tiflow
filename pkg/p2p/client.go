package p2p

import (
	"time"

	p2pImpl "github.com/pingcap/tiflow/pkg/p2p"
	"github.com/pingcap/tiflow/pkg/security"
)

type MessageRouter = p2pImpl.MessageRouter

var defaultClientConfig = &p2pImpl.MessageClientConfig{
	SendChannelSize:         128,
	BatchSendInterval:       100 * time.Millisecond, // essentially disables flushing
	MaxBatchBytes:           8 * 1024 * 1024,        // 8MB
	MaxBatchCount:           4096,
	RetryRateLimitPerSecond: 1.0,      // once per second
	ClientVersion:           "v5.4.0", // a fake version
}

func NewMessageRouter(nodeID NodeID, advertisedAddr string) MessageRouter {
	config := *defaultClientConfig // copy
	config.AdvertisedAddr = advertisedAddr
	return p2pImpl.NewMessageRouter(
		nodeID,
		&security.Credential{ /* TLS not supported for now */ },
		&config,
	)
}
