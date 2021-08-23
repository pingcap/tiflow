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

package capture

import (
	"context"
	"testing"

	"github.com/pingcap/check"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/util/testleak"
)

var _ = check.Suite(&httpValidatorSuite{})

func Test(t *testing.T) { check.TestingT(t) }

type httpValidatorSuite struct {
}

func (s *httpValidatorSuite) TestVerifyUpdateChangefeedConfig(c *check.C) {
	defer testleak.AfterTest(c)()
	ctx := context.Background()
	oldInfo := &model.ChangeFeedInfo{Config: config.GetDefaultReplicaConfig()}
	// test startTs > targetTs
	changefeedConfig := model.ChangefeedConfig{TargetTS: 20}
	oldInfo.StartTs = 40
	newInfo, err := verifyUpdateChangefeedConfig(ctx, changefeedConfig, oldInfo)
	c.Assert(err, check.NotNil)
	c.Assert(newInfo, check.IsNil)

	// test no change error
	changefeedConfig = model.ChangefeedConfig{SinkURI: "blcakhole://"}
	oldInfo.SinkURI = "blcakhole://"
	newInfo, err = verifyUpdateChangefeedConfig(ctx, changefeedConfig, oldInfo)
	c.Assert(err, check.NotNil)
	c.Assert(newInfo, check.IsNil)

	// test verify success
	changefeedConfig = model.ChangefeedConfig{MounterWorkerNum: 32}
	newInfo, err = verifyUpdateChangefeedConfig(ctx, changefeedConfig, oldInfo)
	c.Assert(err, check.IsNil)
	c.Assert(newInfo, check.NotNil)
}

func (s *httpValidatorSuite) TestVerifySink(c *check.C) {
	defer testleak.AfterTest(c)()
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	replicateConfig := config.GetDefaultReplicaConfig()
	opts := make(map[string]string)

	// test sink uri error
	sinkURI := "mysql:127.0.0.1:0000"
	err := verifySink(ctx, sinkURI, replicateConfig, opts)
	c.Assert(err, check.NotNil)

	// test sink uri right
	sinkURI = "blackhole://"
	err = verifySink(ctx, sinkURI, replicateConfig, opts)
	c.Assert(err, check.IsNil)
}
