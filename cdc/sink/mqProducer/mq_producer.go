package mqProducer

import (
	"context"

	"github.com/pingcap/ticdc/cdc/model"
)

// Producer is a interface of mq producer
type Producer interface {
	SendMessage(ctx context.Context, key *model.MqMessageKey, value *model.MqMessageRow, partition int32) error
	BroadcastMessage(ctx context.Context, key *model.MqMessageKey, value *model.MqMessageDDL) error
	SyncBroadcastMessage(ctx context.Context, key *model.MqMessageKey, value *model.MqMessageDDL) error
	GetPartitionNum() int32
	Successes() chan uint64
	PrintStatus(ctx context.Context) error
	Run(ctx context.Context) error
	Close() error
}
