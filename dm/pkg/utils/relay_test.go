// Copyright 2019 PingCAP, Inc.
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

package utils

import (
	"os"
	"strings"
	"testing"

	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/stretchr/testify/require"
)

func TestParseUUIDIndex(t *testing.T) {
	t.Parallel()

	f, err := os.CreateTemp("", "server-uuid.index")
	require.NoError(t, err)
	defer os.Remove(f.Name())

	uuids := []string{
		"c65525fa-c7a3-11e8-a878-0242ac130005.000001",
		"c6ae5afe-c7a3-11e8-a19d-0242ac130006.000002",
		"c65525fa-c7a3-11e8-a878-0242ac130005.000003",
	}

	err = os.WriteFile(f.Name(), []byte(strings.Join(uuids, "\n")), 0o644)
	require.NoError(t, err)

	obtainedUUIDs, err := ParseUUIDIndex(f.Name())
	require.NoError(t, err)
	require.Equal(t, uuids, obtainedUUIDs)

	// test GetUUIDBySuffix
	uuid := uuids[1]
	uuidWS := GetUUIDBySuffix(uuids, uuid[len(uuid)-6:])
	require.Equal(t, uuid, uuidWS)

	uuidWS = GetUUIDBySuffix(uuids, "100000")
	require.Equal(t, "", uuidWS)
}

func TestSuffixForUUID(t *testing.T) {
	t.Parallel()

	cases := []struct {
		uuid           string
		ID             int
		uuidWithSuffix string
	}{
		{"c65525fa-c7a3-11e8-a878-0242ac130005", 1, "c65525fa-c7a3-11e8-a878-0242ac130005.000001"},
		{"c6ae5afe-c7a3-11e8-a19d-0242ac130006", 2, "c6ae5afe-c7a3-11e8-a19d-0242ac130006.000002"},
	}

	for _, cs := range cases {
		uuidWS := AddSuffixForUUID(cs.uuid, cs.ID)
		require.Equal(t, cs.uuidWithSuffix, uuidWS)

		uuidWOS, id, err := ParseRelaySubDir(cs.uuidWithSuffix)
		require.NoError(t, err)
		require.Equal(t, cs.uuid, uuidWOS)
		require.Equal(t, cs.ID, id)

		suffix := SuffixIntToStr(cs.ID)
		hasSuffix := strings.HasSuffix(cs.uuidWithSuffix, suffix)
		require.Equal(t, true, hasSuffix)
	}

	_, _, err := ParseRelaySubDir("uuid-with-out-suffix")
	require.Error(t, err)

	_, _, err = ParseRelaySubDir("uuid-invalid-suffix-len.01")
	require.Error(t, err)

	_, _, err = ParseRelaySubDir("uuid-invalid-suffix-fmt.abc")
	require.Error(t, err)

	_, _, err = ParseRelaySubDir("uuid-invalid-fmt.abc.000001")
	require.Error(t, err)
}

func TestGenFakeRotateEvent(t *testing.T) {
	t.Parallel()

	var (
		nextLogName = "mysql-bin.000123"
		logPos      = uint64(456)
		serverID    = uint32(101)
	)

	ev, err := GenFakeRotateEvent(nextLogName, logPos, serverID)
	require.NoError(t, err)
	require.Equal(t, serverID, ev.Header.ServerID)
	require.Equal(t, uint32(0), ev.Header.Timestamp)
	require.Equal(t, uint32(0), ev.Header.LogPos)
	require.Equal(t, replication.ROTATE_EVENT, ev.Header.EventType)

	evR, ok := ev.Event.(*replication.RotateEvent)
	require.True(t, ok)
	require.Equal(t, nextLogName, string(evR.NextLogName))
	require.Equal(t, logPos, evR.Position)
}
