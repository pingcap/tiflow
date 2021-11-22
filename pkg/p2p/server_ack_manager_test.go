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
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAckManagerGetSet(t *testing.T) {
	t.Parallel()

	am := newAckManager()
	ack := am.Get("node-1", "topic-1")
	require.Equal(t, initAck, ack)

	am.Set("node-1", "topic-1", Seq(5))
	ack = am.Get("node-1", "topic-1")
	require.Equal(t, Seq(5), ack)

	ack = am.Get("node-1", "topic-2")
	require.Equal(t, initAck, ack)

	am.Set("node-1", "topic-2", Seq(7))
	ack = am.Get("node-1", "topic-2")
	require.Equal(t, Seq(7), ack)

	ack = am.Get("node-2", "topic-1")
	require.Equal(t, initAck, ack)

	am.Set("node-2", "topic-1", Seq(8))
	ack = am.Get("node-2", "topic-1")
	require.Equal(t, Seq(8), ack)

	am.Set("node-1", "topic-2", Seq(7))
	ack = am.Get("node-1", "topic-2")
	require.Equal(t, Seq(7), ack)
}

func TestAckManagerRange(t *testing.T) {
	t.Parallel()

	am := newAckManager()

	expected := make(map[Topic]Seq)
	for i := 1; i <= 100; i++ {
		am.Set("node-1", fmt.Sprintf("topic-%d", i), Seq(i+1))
		expected[fmt.Sprintf("topic-%d", i)] = Seq(i + 1)
	}

	actual := make(map[Topic]Seq)
	am.Range("node-1", func(topic Topic, seq Seq) bool {
		actual[topic] = seq
		return true
	})

	require.Equal(t, expected, actual)
}

func TestAckManagerRangeTerminate(t *testing.T) {
	t.Parallel()

	am := newAckManager()

	for i := 1; i <= 100; i++ {
		am.Set("node-1", fmt.Sprintf("topic-%d", i), Seq(i+1))
	}

	counter := 0
	am.Range("node-1", func(topic Topic, seq Seq) bool {
		counter++
		require.LessOrEqual(t, counter, 50)
		return counter != 50
	})
}

func TestAckManagerRangeNotExists(t *testing.T) {
	t.Parallel()

	am := newAckManager()

	for i := 1; i <= 100; i++ {
		am.Set("node-1", fmt.Sprintf("topic-%d", i), Seq(i+1))
	}

	am.Range("node-2", func(topic Topic, seq Seq) bool {
		require.Fail(t, "unreachable")
		return false
	})
}

func TestAckManagerRemove(t *testing.T) {
	t.Parallel()

	am := newAckManager()
	for i := 1; i <= 10; i++ {
		for j := 1; j <= 10; j++ {
			am.Set(fmt.Sprintf("node-%d", i), fmt.Sprintf("topic-%d", j), Seq(i+j))
		}
	}

	ack := am.Get("node-2", "topic-3")
	require.Equal(t, Seq(5), ack)

	ack = am.Get("node-3", "topic-3")
	require.Equal(t, Seq(6), ack)

	am.RemoveTopic("topic-3")

	ack = am.Get("node-2", "topic-3")
	require.Equal(t, initAck, ack)

	ack = am.Get("node-3", "topic-3")
	require.Equal(t, initAck, ack)

	ack = am.Get("node-2", "topic-2")
	require.Equal(t, Seq(4), ack)

	ack = am.Get("node-3", "topic-2")
	require.Equal(t, Seq(5), ack)

	am.RemoveNode("node-2")

	ack = am.Get("node-2", "topic-2")
	require.Equal(t, initAck, ack)
}
