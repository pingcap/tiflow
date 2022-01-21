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

package model

import (
	"github.com/pingcap/check"
	"github.com/pingcap/tiflow/pkg/util/testleak"
)

type captureSuite struct{}

var _ = check.Suite(&captureSuite{})

func (s *captureSuite) TestMarshalUnmarshal(c *check.C) {
	defer testleak.AfterTest(c)()
	info := &CaptureInfo{
		ID:            "9ff52aca-aea6-4022-8ec4-fbee3f2c7890",
		AdvertiseAddr: "127.0.0.1:8300",
		Version:       "dev",
	}
	expected := `{"id":"9ff52aca-aea6-4022-8ec4-fbee3f2c7890","address":"127.0.0.1:8300","version":"dev"}`
	data, err := info.Marshal()
	c.Assert(err, check.IsNil)
	c.Assert(string(data), check.Equals, expected)
	decodedInfo := &CaptureInfo{}
	err = decodedInfo.Unmarshal(data)
	c.Assert(err, check.IsNil)
	c.Assert(decodedInfo, check.DeepEquals, info)
}

func (s *captureSuite) TestListVersionsFromCaptureInfos(c *check.C) {
	defer testleak.AfterTest(c)()
	infos := []*CaptureInfo{
		{
			ID:            "9ff52aca-aea6-4022-8ec4-fbee3f2c7891",
			AdvertiseAddr: "127.0.0.1:8300",
			Version:       "dev",
		},
		{
			ID:            "9ff52aca-aea6-4022-8ec4-fbee3f2c7891",
			AdvertiseAddr: "127.0.0.1:8300",
			Version:       "",
		},
	}

	c.Assert(ListVersionsFromCaptureInfos(infos), check.DeepEquals, []string{"dev", ""})
}
