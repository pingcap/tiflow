// Copyright 2020 PingCAP, Inc.
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

package mqProducer

import (
	"testing"

	"github.com/pingcap/check"
)

type kafkaSuite struct{}

var _ = check.Suite(&kafkaSuite{})

func Test(t *testing.T) { check.TestingT(t) }

func (s *kafkaSuite) TestClientID(c *check.C) {
	testCases := []struct {
		role         string
		addr         string
		changefeedID string
		configuredID string
		hasError     bool
		expected     string
	}{
		{"owner", "domain:1234", "123-121-121-121", "", false, "TiCDC_sarama_producer_owner_domain_1234_123-121-121-121"},
		{"owner", "127.0.0.1:1234", "123-121-121-121", "", false, "TiCDC_sarama_producer_owner_127.0.0.1_1234_123-121-121-121"},
		{"owner", "127.0.0.1:1234?:,\"", "123-121-121-121", "", false, "TiCDC_sarama_producer_owner_127.0.0.1_1234_____123-121-121-121"},
		{"owner", "中文", "123-121-121-121", "", true, ""},
		{"owner", "127.0.0.1:1234", "123-121-121-121", "cdc-changefeed-1", false, "cdc-changefeed-1"},
	}
	for _, tc := range testCases {
		id, err := kafkaClientID(tc.role, tc.addr, tc.changefeedID, tc.configuredID)
		if tc.hasError {
			c.Assert(err, check.NotNil)
		} else {
			c.Assert(err, check.IsNil)
			c.Assert(id, check.Equals, tc.expected)
		}
	}
}
