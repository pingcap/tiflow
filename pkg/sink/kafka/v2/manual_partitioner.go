package v2

import "github.com/segmentio/kafka-go"

type manualPartitioner struct{}

func newManualPartitioner() kafka.Balancer {
	return &manualPartitioner{}
}

func (m manualPartitioner) Balance(msg kafka.Message, partitions ...int) (partition int) {
	return msg.Partition
}
