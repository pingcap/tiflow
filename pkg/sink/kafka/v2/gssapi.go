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
	"encoding/asn1"
	"encoding/binary"

	"github.com/jcmturner/gokrb5/v8/client"
	"github.com/jcmturner/gokrb5/v8/credentials"
	"github.com/jcmturner/gokrb5/v8/crypto"
	"github.com/jcmturner/gokrb5/v8/gssapi"
	"github.com/jcmturner/gokrb5/v8/iana/chksumtype"
	"github.com/jcmturner/gokrb5/v8/iana/keyusage"
	"github.com/jcmturner/gokrb5/v8/messages"
	"github.com/jcmturner/gokrb5/v8/types"
	"github.com/pingcap/errors"
	"github.com/segmentio/kafka-go/sasl"
)

const (
	// TokIDKrbApReq https://tools.ietf.org/html/rfc4121#section-4.1
	TokIDKrbApReq = "\x01\x00"
)

// Gokrb5v8Client is the client for gokrbv8
type Gokrb5v8Client interface {
	// GetServiceTicket get a ticker form server
	GetServiceTicket(spn string) (messages.Ticket, types.EncryptionKey, error)
	// Destroy stops the auto-renewal of all sessions and removes
	// the sessions and cache entries from the client.
	Destroy()
	// Credentials returns the client credentials
	Credentials() *credentials.Credentials
}

type gokrb5v8ClientImpl struct {
	client *client.Client
}

func (c *gokrb5v8ClientImpl) GetServiceTicket(spn string) (
	messages.Ticket, types.EncryptionKey, error,
) {
	return c.client.GetServiceTicket(spn)
}

func (c *gokrb5v8ClientImpl) Credentials() *credentials.Credentials {
	return c.client.Credentials
}

func (c *gokrb5v8ClientImpl) Destroy() {
	c.client.Destroy()
}

type mechanism struct {
	client      Gokrb5v8Client
	serviceName string
	host        string
}

func (m mechanism) Name() string {
	return "GSSAPI"
}

// Gokrb5v8 uses gokrb5/v8 to implement the GSSAPI mechanism.
//
// client is a github.com/gokrb5/v8/client *Client instance.
// kafkaServiceName is the name of the Kafka service in your Kerberos.
func Gokrb5v8(client Gokrb5v8Client, kafkaServiceName string) sasl.Mechanism {
	return mechanism{client, kafkaServiceName, ""}
}

// StartWithoutHostError is the error type for when Start is called on
// the GSSAPI mechanism without the host having been set by WithHost.
//
// Unless you are calling the GSSAPI SASL mechanim's Start method
// yourself for some reason, this error will never be returned.
type StartWithoutHostError struct{}

func (e StartWithoutHostError) Error() string {
	return "GSSAPI SASL handshake needs a host"
}

func (m mechanism) Start(ctx context.Context) (sasl.StateMachine, []byte, error) {
	metaData := sasl.MetadataFromContext(ctx)
	m.host = metaData.Host
	if m.host == "" {
		return nil, nil, StartWithoutHostError{}
	}

	servicePrincipalName := m.serviceName + "/" + m.host
	ticket, key, err := m.client.GetServiceTicket(
		servicePrincipalName,
	)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}

	authenticator, err := types.NewAuthenticator(
		m.client.Credentials().Realm(),
		m.client.Credentials().CName(),
	)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}

	encryptionType, err := crypto.GetEtype(key.KeyType)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}

	keySize := encryptionType.GetKeyByteSize()
	err = authenticator.GenerateSeqNumberAndSubKey(key.KeyType, keySize)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}

	authenticator.Cksum = types.Checksum{
		CksumType: chksumtype.GSSAPI,
		Checksum:  authenticatorPseudoChecksum(),
	}
	apReq, err := messages.NewAPReq(ticket, key, authenticator)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}

	bytes, err := apReq.Marshal()
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	gssapiToken, err := getGssAPIToken(bytes)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	return &gokrb5v8Session{authenticator.SubKey, false}, gssapiToken, nil
}

func getGssAPIToken(bytes []byte) ([]byte, error) {
	bytesWithPrefix := make([]byte, 0, len(TokIDKrbApReq)+len(bytes))
	bytesWithPrefix = append(bytesWithPrefix, TokIDKrbApReq...)
	bytesWithPrefix = append(bytesWithPrefix, bytes...)

	return prependGSSAPITokenTag(bytesWithPrefix)
}

func authenticatorPseudoChecksum() []byte {
	// Not actually a checksum, but it goes in the checksum field.
	// https://tools.ietf.org/html/rfc4121#section-4.1.1
	checksum := make([]byte, 24)

	flags := gssapi.ContextFlagInteg
	// Reasons for each flag being on or off:
	//     Delegation: Off. We are not using delegated credentials.
	//     Mutual: Off. Mutual authentication is already provided
	//         as a result of how Kerberos works.
	//     Replay: Off. We don’t need replay protection because each
	//         packet is secured by a per-session key and is unique
	//         within its session.
	//     Sequence: Off. Out-of-order messages cannot happen in our
	//         case, and if it somehow happened anyway it would
	//         necessarily trigger other appropriate errors.
	//     Confidentiality: Off. Our authentication itself does not
	//         seem to be requesting or using any “security layers”
	//         in the GSSAPI sense, and this is just one of the
	//         security layer features. Also, if we were requesting
	//         a GSSAPI security layer, we would be required to
	//         set the mutual flag to on.
	//         https://tools.ietf.org/html/rfc4752#section-3.1
	//     Integrity: On. Must be on when calling the standard API,
	//         so it probably must be set in the raw packet itself.
	//         https://tools.ietf.org/html/rfc4752#section-3.1
	//         https://tools.ietf.org/html/rfc4752#section-7
	//     Anonymous: Off. We are not using an anonymous ticket.
	//         https://tools.ietf.org/html/rfc6112#section-3

	binary.LittleEndian.PutUint32(checksum[0:4], 16)
	// checksum[4:20] is unused/blank channel binding settings.
	binary.LittleEndian.PutUint32(checksum[20:24], uint32(flags))
	return checksum
}

type gssapiToken struct {
	OID    asn1.ObjectIdentifier
	Object asn1.RawValue
}

func prependGSSAPITokenTag(payload []byte) ([]byte, error) {
	// The GSSAPI "token" is almost an ASN.1 encoded object, except
	// that the "token object" is raw bytes, not necessarily ASN.1.
	// https://tools.ietf.org/html/rfc2743#page-81 (section 3.1)
	token := gssapiToken{
		OID:    asn1.ObjectIdentifier(gssapi.OIDKRB5.OID()),
		Object: asn1.RawValue{FullBytes: payload},
	}
	return asn1.MarshalWithParams(token, "application")
}

type gokrb5v8Session struct {
	key  types.EncryptionKey
	done bool
}

func (s *gokrb5v8Session) Next(ctx context.Context, challenge []byte) (bool, []byte, error) {
	if s.done {
		return true, nil, nil
	}
	const tokenIsFromGSSAcceptor = true
	challengeToken := gssapi.WrapToken{}
	err := challengeToken.Unmarshal(challenge, tokenIsFromGSSAcceptor)
	if err != nil {
		return false, nil, errors.Trace(err)
	}

	valid, err := challengeToken.Verify(
		s.key,
		keyusage.GSSAPI_ACCEPTOR_SEAL,
	)
	if !valid {
		return false, nil, errors.Trace(err)
	}

	responseToken, err := gssapi.NewInitiatorWrapToken(
		challengeToken.Payload,
		s.key,
	)
	if err != nil {
		return false, nil, errors.Trace(err)
	}

	response, err := responseToken.Marshal()
	if err != nil {
		return false, nil, errors.Trace(err)
	}

	// We are done, but we can't return `true` yet because
	// the SASL loop calling this needs the first return to be
	// `false` any time there are response bytes to send.
	s.done = true
	return false, response, nil
}
