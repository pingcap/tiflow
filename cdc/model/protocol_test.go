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

package model

import (
	"testing"

	"github.com/pingcap/ticdc/pkg/p2p"
	"github.com/stretchr/testify/require"
)

// Asserts that SyncMessage implements Serializable, so that
// it can be used by pkg/p2p.
var _ p2p.Serializable = (*SyncMessage)(nil)

// TestChangefeedNameCannotIncludeSlash asserts that changefeed names cannot include slash.
// Or otherwise the topic name encoding would be problematic.
func TestChangefeedNameCannotIncludeSlash(t *testing.T) {
	err := ValidateChangefeedID("a/b")
	require.Error(t, err, "changefeed name cannot include slash")
}

// TestSerializeSyncMessage tests that SyncMessage can be serialized and deserialized.
// SyncMessage is especial since it can be a very large message and it is serialized
// into MsgPack.
func TestSerializeSyncMessage(t *testing.T) {
	largeMessage := makeVeryLargeSyncMessage()
	largeMessageBytes, err := largeMessage.Marshal()
	require.NoError(t, err)
	// Asserts that the message should not be larger than 10MB.
	require.Less(t, len(largeMessageBytes), 10*1024*1024)

	var newSyncMessage SyncMessage
	err = newSyncMessage.Unmarshal(largeMessageBytes)
	require.NoError(t, err)
	require.EqualValues(t, largeMessage, &newSyncMessage)
}

func makeVeryLargeSyncMessage() *SyncMessage {
	largeSliceFn := func () (ret []TableID) {
		for i := 0; i < 80000; i++ {
			ret = append(ret, TableID(i))
        }
		return
	}
    return &SyncMessage{
		Running:  largeSliceFn(),
		Adding:   largeSliceFn(),
		Removing: largeSliceFn(),
	}
}
