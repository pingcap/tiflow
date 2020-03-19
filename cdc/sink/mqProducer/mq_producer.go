package mqProducer

import "context"

// Producer is a interface of mq producer
type Producer interface {
	SendMessage(ctx context.Context, key []byte, value []byte, partition int32, callback func(err error)) (uint64, error)
	BroadcastMessage(ctx context.Context, key []byte, value []byte, callback func(err error)) (uint64, error)
	SyncBroadcastMessage(ctx context.Context, key []byte, value []byte) error
	MaxSuccessesIndex() uint64
	GetPartitionNum() int32
	Run(ctx context.Context) error
	Close() error
}
