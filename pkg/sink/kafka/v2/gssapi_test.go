// Copyright 2023 PingCAP, Inc.
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

package v2

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/jcmturner/gokrb5/v8/credentials"
	"github.com/jcmturner/gokrb5/v8/gssapi"
	"github.com/jcmturner/gokrb5/v8/iana/nametype"
	"github.com/jcmturner/gokrb5/v8/messages"
	"github.com/jcmturner/gokrb5/v8/types"
	"github.com/pingcap/errors"
	mock "github.com/pingcap/tiflow/pkg/sink/kafka/v2/mock"
	"github.com/segmentio/kafka-go/sasl"
	"github.com/stretchr/testify/require"
)

const (
	// What a kerberized server might send
	testChallengeFromAcceptor = "050401ff000c0000000000" +
		"00575e85d601010000853b728d5268525a1386c19f"
	// session key used to sign the tokens above
	sessionKey     = "14f9bde6b50ec508201a97f74c4e5bd3"
	sessionKeyType = 17
)

func getSessionKey() types.EncryptionKey {
	key, _ := hex.DecodeString(sessionKey)
	return types.EncryptionKey{
		KeyType:  sessionKeyType,
		KeyValue: key,
	}
}

func getChallengeReference() *gssapi.WrapToken {
	challenge, _ := hex.DecodeString(testChallengeFromAcceptor)
	return &gssapi.WrapToken{
		Flags:     0x01,
		EC:        12,
		RRC:       0,
		SndSeqNum: binary.BigEndian.Uint64(challenge[8:16]),
		Payload:   []byte{0x01, 0x01, 0x00, 0x00},
		CheckSum:  challenge[20:32],
	}
}

func TestStart(t *testing.T) {
	ctrl := gomock.NewController(t)
	client := mock.NewMockGokrb5v8Client(ctrl)
	m := Gokrb5v8(client, "kafka")
	require.Equal(t, "GSSAPI", m.Name())
	_, _, err := m.Start(sasl.WithMetadata(context.Background(),
		&sasl.Metadata{}))
	require.Equal(t, err, StartWithoutHostError{})

	ctx := sasl.WithMetadata(context.Background(), &sasl.Metadata{
		Host: "localhost",
		Port: 9092,
	})
	client.EXPECT().GetServiceTicket(gomock.Any()).Return(
		messages.Ticket{}, getSessionKey(), errors.New("fake errors"))
	_, _, err = m.Start(ctx)
	require.Contains(t, "fake errors", err.Error())

	client.EXPECT().GetServiceTicket(gomock.Any()).Return(
		messages.Ticket{
			SName: types.NewPrincipalName(nametype.KRB_NT_PRINCIPAL,
				"kafka@EXAMPLE.COM"),
		}, getSessionKey(), nil)
	client.EXPECT().Credentials().AnyTimes().
		Return(credentials.New("user", "kafka"))
	stm, data, err := m.Start(ctx)
	require.NotNil(t, stm)
	require.NotNil(t, data)
	require.Nil(t, err)
}

func TestNext(t *testing.T) {
	session := gokrb5v8Session{key: getSessionKey()}
	wrapToken := getChallengeReference()
	data, err := wrapToken.Marshal()
	require.Nil(t, err)
	ok, resp, err := session.Next(context.Background(), data)
	require.False(t, ok)
	require.NotNil(t, resp)
	require.Nil(t, err)
	ok, resp, err = session.Next(context.Background(), data)
	require.True(t, ok)
	require.Nil(t, resp)
	require.Nil(t, err)
}
