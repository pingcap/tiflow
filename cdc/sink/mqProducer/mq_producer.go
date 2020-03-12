package mqProducer

import "context"

// Producer is a interface of mq producer
type Producer interface {
	SendMessage(ctx context.Context, key []byte, value []byte, partition int32) error
	BroadcastMessage(ctx context.Context, key []byte, value []byte) error
	GetPartitionNum() int32
	Close() error
}
