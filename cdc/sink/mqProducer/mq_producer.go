package mqProducer

import (
	"context"
)

// Producer is a interface of mq producer
type Producer interface {
	SendMessage(ctx context.Context, key []byte, value []byte, partition int32) error
	SyncBroadcastMessage(ctx context.Context, key []byte, value []byte) error
	Flush(ctx context.Context) error
	GetPartitionNum() int32
	Close() error
}
