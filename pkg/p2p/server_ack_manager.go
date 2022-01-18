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

import "sync"

// Background:
//
// Peer-messages are divided into topics to facilitate registering handlers and
// to avoid interference between different types of messages.
//
// For a given (sender, topic) pair, messages are handled in order. In the case
// of a retry after failure, we need to know the latest progress so that the client
// can retry from that message. This is what we need Acks for.

// ackManager is used to track the progress of Acks.
// It is thread-safe to use.
type ackManager struct {
	peers sync.Map
}

const (
	// initAck is the initial value of an Ack.
	// It is a placeholder for unknown progress.
	initAck = Seq(0)
)

type peerAckList struct {
	mu   sync.RWMutex
	acks map[Topic]Seq
}

// newAckManager returns a new ackManager
func newAckManager() *ackManager {
	return &ackManager{}
}

// Get returns the latest ACK for a given topic sent from a given node.
func (m *ackManager) Get(senderID NodeID, topic Topic) Seq {
	rawAcks, ok := m.peers.Load(senderID)
	if !ok {
		return initAck
	}

	ackList := rawAcks.(*peerAckList)
	ackList.mu.RLock()
	defer ackList.mu.RUnlock()

	return ackList.acks[topic]
}

// Set sets the latest ACK for a given topic sent from a given node.
func (m *ackManager) Set(senderID NodeID, topic Topic, newSeq Seq) {
	rawAcks, ok := m.peers.Load(senderID)
	if !ok {
		newAcks := &peerAckList{
			acks: make(map[Topic]Seq),
		}
		// LoadOrStore will load the existing value if another thread
		// has just inserted the value for our key.
		rawAcks, _ = m.peers.LoadOrStore(senderID, newAcks)
	}

	ackList := rawAcks.(*peerAckList)
	ackList.mu.Lock()
	defer ackList.mu.Unlock()
	ackList.acks[topic] = newSeq
}

// Range iterates through all the topics from a given node.
// The iteration terminates if fn returns false.
func (m *ackManager) Range(senderID NodeID, fn func(topic Topic, seq Seq) bool) {
	rawAcks, ok := m.peers.Load(senderID)
	if !ok {
		return
	}

	ackList := rawAcks.(*peerAckList)
	ackList.mu.RLock()
	defer ackList.mu.RUnlock()

	for topic, seq := range ackList.acks {
		if !fn(topic, seq) {
			return
		}
	}
}

// RemoveTopic removes a topic for all nodes.
// We do not support removing a topic from a specific node.
func (m *ackManager) RemoveTopic(topic Topic) {
	m.peers.Range(func(key, value interface{}) bool {
		ackList := value.(*peerAckList)
		ackList.mu.Lock()
		defer ackList.mu.Unlock()
		delete(ackList.acks, topic)
		return true
	})
}

// RemoveNode removes all records of ACKS for a given node.
func (m *ackManager) RemoveNode(senderID NodeID) {
	m.peers.Delete(senderID)
}
