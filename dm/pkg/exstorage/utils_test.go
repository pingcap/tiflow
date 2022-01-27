// Copyright 2022 PingCAP, Inc.
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

package exstorage

import (
	"testing"

	. "github.com/pingcap/check"
)

func TestT(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testS3UtilsSuite{})

type testS3UtilsSuite struct{}

func (s *testS3UtilsSuite) TestAdjustS3PathAndIsS3(c *C) {
	testPaths := []string{
		"",
		"1invalid:",
		"file:///tmp/storage",
		"/tmp/storage",
		"./tmp/storage",
		"tmp/storage",
		"s3:///bucket/more/prefix",
		"s3://bucket2/prefix",
		"s3://bucket3/prefix/path?endpoint=https://127.0.0.1:9000&force_path_style=0&SSE=aws:kms&sse-kms-key-id=TestKey&xyz=abc",
		"s3://bucket4/prefix/path?access-key=NXN7IPIOSAAKDEEOLMAF&secret-access-key=nREY/7Dt+PaIbYKrKlEEMMF/ExCiJEX=XMLPUANw",
		"s3://bucket4/prefix/path.mysql-replica-01?access-key=NXN7IPIOSAAKDEEOLMAF&secret-access-key=nREY/7Dt+PaIbYKrKlEEMMF/ExCiJEX=XMLPUANw",
	}

	testAjustResults := []struct {
		hasErr bool
		res    string
	}{
		{false, ""},
		{true, "parse (.*)1invalid:(.*): first path segment in URL cannot contain colon*"},
		{false, "file:///tmp/storage"},
		{false, "/tmp/storage"},
		{false, "./tmp/storage"},
		{false, "tmp/storage"},
		{false, "s3:///bucket/more/prefix.mysql-replica-01"},
		{false, "s3://bucket2/prefix.mysql-replica-01"},
		{false, "s3://bucket3/prefix/path.mysql-replica-01?endpoint=https://127.0.0.1:9000&force_path_style=0&SSE=aws:kms&sse-kms-key-id=TestKey&xyz=abc"},
		{false, "s3://bucket4/prefix/path.mysql-replica-01?access-key=NXN7IPIOSAAKDEEOLMAF&secret-access-key=nREY/7Dt+PaIbYKrKlEEMMF/ExCiJEX=XMLPUANw"},
		{false, "s3://bucket4/prefix/path.mysql-replica-01?access-key=NXN7IPIOSAAKDEEOLMAF&secret-access-key=nREY/7Dt+PaIbYKrKlEEMMF/ExCiJEX=XMLPUANw"},
	}

	for i, testPath := range testPaths {
		newDir, err := AdjustS3Path(testPath, "mysql-replica-01")
		if testAjustResults[i].hasErr {
			c.Assert(err, NotNil)
			c.Assert(err.Error(), Matches, testAjustResults[i].res)
		} else {
			c.Assert(err, IsNil)
			c.Assert(newDir, Equals, testAjustResults[i].res)
		}
	}

	testIsS3Results := []struct {
		hasErr bool
		res    bool
		errMsg string
	}{
		{false, false, ""},
		{true, false, "parse (.*)1invalid:(.*): first path segment in URL cannot contain colon*"},
		{false, false, ""},
		{false, false, ""},
		{false, false, ""},
		{false, false, ""},
		{false, true, ""},
		{false, true, ""},
		{false, true, ""},
		{false, true, ""},
		{false, true, ""},
	}

	for i, testPath := range testPaths {
		isS3, err := IsS3Path(testPath)
		if testIsS3Results[i].hasErr {
			c.Assert(err, NotNil)
			c.Assert(err.Error(), Matches, testIsS3Results[i].errMsg)
		} else {
			c.Assert(err, IsNil)
			c.Assert(isS3, Equals, testIsS3Results[i].res)
		}
	}
}
