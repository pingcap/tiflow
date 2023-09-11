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

package pulsar

import (
	"net/url"
	"testing"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/stretchr/testify/assert"
)

func TestPulsarConfig(t *testing.T) {
	p := &config.PulsarConfig{
		CompressionType:         (*config.PulsarCompressionType)(aws.String("lz4")),
		ConnectionTimeout:       (*config.TimeSec)(aws.Int(defaultConnectionTimeout)),
		OperationTimeout:        (*config.TimeSec)(aws.Int(998)),
		BatchingMaxMessages:     aws.Uint(defaultBatchingMaxSize),
		BatchingMaxPublishDelay: (*config.TimeMill)(aws.Int(defaultBatchingMaxPublishDelay)),
		SendTimeout:             (*config.TimeSec)(aws.Int(123)),
	}

	// Define test cases
	tests := []struct {
		name    string
		sinkURI string
		wantErr bool
	}{
		{
			name: "Test valid sinkURI",
			sinkURI: "pulsar://127.0.0.1:6650/persistent://tenant/namespace/test-topic" +
				"?send-timeout=0123&compression=lz4&operation-timeout=998",
			wantErr: false,
		},
		{
			name:    "Test invalid sinkURI",
			sinkURI: "pulsar://?send-timeout=123&compression=lz4",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sink, err := url.Parse(tt.sinkURI)
			if err != nil {
				t.Fatalf("Failed to parse sinkURI: %v", err)
			}

			replicaConfig := config.GetDefaultReplicaConfig()

			replicaConfig.Sink.PulsarConfig = p
			// Call function under test
			config, err := NewPulsarConfig(sink, replicaConfig.Sink.PulsarConfig)

			// Assert error value
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}

			// If no error is expected, assert config values
			if !tt.wantErr {
				assert.Equal(t, config.CompressionType.Value(), pulsar.LZ4)
				assert.Equal(t, config.BrokerURL, "pulsar://127.0.0.1:6650")
				assert.Equal(t, config.ConnectionTimeout.Duration(), defaultConnectionTimeout*time.Second)
				assert.Equal(t, config.OperationTimeout.Duration(), 998*time.Second)
				assert.Equal(t, *config.BatchingMaxMessages, defaultBatchingMaxSize)
				assert.Equal(t, config.BatchingMaxPublishDelay.Duration(), defaultBatchingMaxPublishDelay*time.Millisecond)
				assert.Equal(t, config.SendTimeout.Duration(), 123*time.Second)
			}
		})
	}
}

func TestGetBrokerURL(t *testing.T) {
	sink, _ := url.Parse("pulsar://localhost:6650/test")

	replicaConfig := config.GetDefaultReplicaConfig()
	config, _ := NewPulsarConfig(sink, replicaConfig.Sink.PulsarConfig)

	assert.Equal(t, config.BrokerURL, "pulsar://localhost:6650")
}

func TestGetSinkURI(t *testing.T) {
	sink, _ := url.Parse("pulsar://127.0.0.1:6650/persistent://tenant/namespace/test-topic" +
		"?max-message-bytes=5000&compression=lz4")

	replicaConfig := config.GetDefaultReplicaConfig()
	config, _ := NewPulsarConfig(sink, replicaConfig.Sink.PulsarConfig)

	assert.Equal(t, config.SinkURI, sink)
}

func TestGetDefaultTopicName(t *testing.T) {
	sink, _ := url.Parse("pulsar://localhost:6650/test")
	replicaConfig := config.GetDefaultReplicaConfig()
	config, _ := NewPulsarConfig(sink, replicaConfig.Sink.PulsarConfig)
	assert.Equal(t, config.GetDefaultTopicName(), "test")

	sink, _ = url.Parse("pulsar://127.0.0.1:6650/persistent://tenant/namespace/test-topic")
	config, _ = NewPulsarConfig(sink, replicaConfig.Sink.PulsarConfig)
	assert.Equal(t, config.GetDefaultTopicName(), "persistent://tenant/namespace/test-topic")
}
