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

package kafka

import (
	"context"
	"fmt"
	"net/url"
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/cdc/model"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/stretchr/testify/require"
)

func TestCompleteOptions(t *testing.T) {
	options := NewOptions()

	// Normal config.
	uriTemplate := "kafka://127.0.0.1:9092/kafka-test?kafka-version=2.6.0&max-batch-size=5" +
		"&max-message-bytes=%s&partition-num=1&replication-factor=3" +
		"&kafka-client-id=unit-test&auto-create-topic=false&compression=gzip"
	maxMessageSize := "4096" // 4kb
	uri := fmt.Sprintf(uriTemplate, maxMessageSize)
	sinkURI, err := url.Parse(uri)
	require.NoError(t, err)

	ctx := context.Background()
	err = options.Apply(ctx, sinkURI)
	require.NoError(t, err)
	require.Equal(t, int32(1), options.PartitionNum)
	require.Equal(t, int16(3), options.ReplicationFactor)
	require.Equal(t, "2.6.0", options.Version)
	require.Equal(t, 4096, options.MaxMessageBytes)

	// multiple kafka broker endpoints
	uri = "kafka://127.0.0.1:9092,127.0.0.1:9091,127.0.0.1:9090/kafka-test?"
	sinkURI, err = url.Parse(uri)
	require.NoError(t, err)
	options = NewOptions()
	err = options.Apply(ctx, sinkURI)
	require.NoError(t, err)
	require.Len(t, options.BrokerEndpoints, 3)

	// Illegal replication-factor.
	uri = "kafka://127.0.0.1:9092/abc?kafka-version=2.6.0&replication-factor=a"
	sinkURI, err = url.Parse(uri)
	require.NoError(t, err)
	options = NewOptions()
	err = options.Apply(ctx, sinkURI)
	require.Regexp(t, ".*invalid syntax.*", errors.Cause(err))

	// Illegal max-message-bytes.
	uri = "kafka://127.0.0.1:9092/abc?kafka-version=2.6.0&max-message-bytes=a"
	sinkURI, err = url.Parse(uri)
	require.NoError(t, err)
	options = NewOptions()
	err = options.Apply(ctx, sinkURI)
	require.Regexp(t, ".*invalid syntax.*", errors.Cause(err))

	// Illegal partition-num.
	uri = "kafka://127.0.0.1:9092/abc?kafka-version=2.6.0&partition-num=a"
	sinkURI, err = url.Parse(uri)
	require.NoError(t, err)
	options = NewOptions()
	err = options.Apply(ctx, sinkURI)
	require.Regexp(t, ".*invalid syntax.*", errors.Cause(err))

	// Out of range partition-num.
	uri = "kafka://127.0.0.1:9092/abc?kafka-version=2.6.0&partition-num=0"
	sinkURI, err = url.Parse(uri)
	require.NoError(t, err)
	options = NewOptions()
	err = options.Apply(ctx, sinkURI)
	require.Regexp(t, ".*invalid partition num.*", errors.Cause(err))
}

func TestSetPartitionNum(t *testing.T) {
	options := NewOptions()
	err := options.SetPartitionNum(2)
	require.NoError(t, err)
	require.Equal(t, int32(2), options.PartitionNum)

	options.PartitionNum = 1
	err = options.SetPartitionNum(2)
	require.NoError(t, err)
	require.Equal(t, int32(1), options.PartitionNum)

	options.PartitionNum = 3
	err = options.SetPartitionNum(2)
	require.True(t, cerror.ErrKafkaInvalidPartitionNum.Equal(err))
}

func TestClientID(t *testing.T) {
	testCases := []struct {
		role         string
		addr         string
		changefeedID string
		configuredID string
		hasError     bool
		expected     string
	}{
		{
			"owner", "domain:1234", "123-121-121-121",
			"", false,
			"TiCDC_producer_owner_domain_1234_default_123-121-121-121",
		},
		{
			"owner", "127.0.0.1:1234", "123-121-121-121",
			"", false,
			"TiCDC_producer_owner_127.0.0.1_1234_default_123-121-121-121",
		},
		{
			"owner", "127.0.0.1:1234?:,\"", "123-121-121-121",
			"", false,
			"TiCDC_producer_owner_127.0.0.1_1234_____default_123-121-121-121",
		},
		{
			"owner", "中文", "123-121-121-121",
			"", true, "",
		},
		{
			"owner", "127.0.0.1:1234",
			"123-121-121-121", "cdc-changefeed-1", false,
			"cdc-changefeed-1",
		},
	}
	for _, tc := range testCases {
		id, err := newKafkaClientID(tc.role, tc.addr,
			model.DefaultChangeFeedID(tc.changefeedID), tc.configuredID)
		if tc.hasError {
			require.Error(t, err)
		} else {
			require.NoError(t, err)
			require.Equal(t, tc.expected, id)
		}
	}
}

func TestTimeout(t *testing.T) {
	options := NewOptions()
	require.Equal(t, 10*time.Second, options.DialTimeout)
	require.Equal(t, 10*time.Second, options.ReadTimeout)
	require.Equal(t, 10*time.Second, options.WriteTimeout)

	uri := "kafka://127.0.0.1:9092/kafka-test?dial-timeout=5s&read-timeout=1000ms" +
		"&write-timeout=2m"
	sinkURI, err := url.Parse(uri)
	require.NoError(t, err)

	err = options.Apply(context.Background(), sinkURI)
	require.NoError(t, err)

	require.Equal(t, 5*time.Second, options.DialTimeout)
	require.Equal(t, 1000*time.Millisecond, options.ReadTimeout)
	require.Equal(t, 2*time.Minute, options.WriteTimeout)
}
