// Copyright 2021 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package p2p

import (
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	proto "github.com/pingcap/ticdc/proto/p2p"
)

const (
	// The maximum size of pre-allocated buffer.
	// 4096 is reasonable given the scenarios under which
	// the peer-message system is used.
	maxPreallocBatchSize = 4096
)

type (
	messageEntry = *proto.MessageEntry
	clientStream = proto.CDCPeerToPeer_SendMessageClient
)

// clientBatchSender is a batch sender that
// batches messages and sends them through a gRPC client.
type clientBatchSender interface {
	Append(msg messageEntry) error
	Flush() error
}

type clientBatchSenderImpl struct {
	stream clientStream

	buffer    []messageEntry
	sizeBytes int

	maxEntryCount int
	maxSizeBytes  int
}

func newClientBatchSender(stream clientStream, maxEntryCount, maxSizeBytes int) clientBatchSender {
	sliceCap := maxEntryCount
	if sliceCap > maxPreallocBatchSize {
		sliceCap = maxPreallocBatchSize
	}
	return &clientBatchSenderImpl{
		stream:        stream,
		buffer:        make([]messageEntry, 0, sliceCap),
		maxEntryCount: maxEntryCount,
		maxSizeBytes:  maxSizeBytes,
	}
}

// Append appends a message to the batch. If the resulting batch contains more than
// maxEntryCount messages or the total size of messages exceeds maxSizeBytes,
// the batch is flushed.
func (s *clientBatchSenderImpl) Append(msg messageEntry) error {
	failpoint.Inject("ClientBatchSenderInjectError", func() {
		failpoint.Return(errors.New("injected error"))
	})

	s.buffer = append(s.buffer, msg)
	s.sizeBytes += msg.Size()

	if len(s.buffer) >= s.maxEntryCount || s.sizeBytes >= s.maxSizeBytes {
		return s.Flush()
	}
	return nil
}

// Flush flushes the batch.
func (s *clientBatchSenderImpl) Flush() error {
	failpoint.Inject("ClientBatchSenderInjectError", func() {
		failpoint.Return(errors.New("injected error"))
	})

	if len(s.buffer) == 0 {
		return nil
	}

	var messagePacket proto.MessagePacket
	messagePacket.Entries = s.buffer
	err := s.stream.Send(&messagePacket)
	s.sizeBytes = 0
	s.buffer = s.buffer[:0]
	return err
}
