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

func (s *testS3UtilsSuite) TestAdjustS3Path(c *C) {
	_, _, err := AdjustS3Path("1invalid:", "")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Matches, "parse (.*)1invalid:(.*): first path segment in URL cannot contain colon")

	isS3, newURL, err := AdjustS3Path("file:///tmp/storage", "mysql-replica-01")
	c.Assert(err, IsNil)
	c.Assert(isS3, IsFalse)
	c.Assert(newURL, Equals, "file:///tmp/storage")

	isS3, newURL, err = AdjustS3Path("s3:///bucket/more/prefix", "")
	c.Assert(err, IsNil)
	c.Assert(isS3, IsTrue)
	c.Assert(newURL, Equals, "s3:///bucket/more/prefix")

	isS3, newURL, err = AdjustS3Path("", "mysql-replica-01")
	c.Assert(err, IsNil)
	c.Assert(isS3, IsFalse)
	c.Assert(newURL, Equals, "")

	isS3, newURL, err = AdjustS3Path("", "")
	c.Assert(err, IsNil)
	c.Assert(isS3, IsFalse)
	c.Assert(newURL, Equals, "")

	isS3, newURL, err = AdjustS3Path("s3:///bucket/more/prefix", "mysql-replica-01")
	c.Assert(err, IsNil)
	c.Assert(isS3, IsTrue)
	c.Assert(newURL, Equals, "s3:///bucket/more/prefix.mysql-replica-01")

	isS3, newURL, err = AdjustS3Path("s3://bucket2/prefix", "mysql-replica-01")
	c.Assert(err, IsNil)
	c.Assert(isS3, IsTrue)
	c.Assert(newURL, Equals, "s3://bucket2/prefix.mysql-replica-01")

	isS3, newURL, err = AdjustS3Path(`s3://bucket3/prefix/path?endpoint=https://127.0.0.1:9000&force_path_style=0&SSE=aws:kms&sse-kms-key-id=TestKey&xyz=abc`, "mysql-replica-01")
	c.Assert(err, IsNil)
	c.Assert(isS3, IsTrue)
	c.Assert(newURL, Equals, "s3://bucket3/prefix/path.mysql-replica-01?endpoint=https://127.0.0.1:9000&force_path_style=0&SSE=aws:kms&sse-kms-key-id=TestKey&xyz=abc")

	// special character in access keys
	isS3, newURL, err = AdjustS3Path(`s3://bucket4/prefix/path?access-key=NXN7IPIOSAAKDEEOLMAF&secret-access-key=nREY/7Dt+PaIbYKrKlEEMMF/ExCiJEX=XMLPUANw`, "mysql-replica-01")
	c.Assert(err, IsNil)
	c.Assert(isS3, IsTrue)
	c.Assert(newURL, Equals, "s3://bucket4/prefix/path.mysql-replica-01?access-key=NXN7IPIOSAAKDEEOLMAF&secret-access-key=nREY/7Dt+PaIbYKrKlEEMMF/ExCiJEX=XMLPUANw")

	// duplicate uniqueID
	isS3, newURL, err = AdjustS3Path(`s3://bucket4/prefix/path.mysql-replica-01?access-key=NXN7IPIOSAAKDEEOLMAF&secret-access-key=nREY/7Dt+PaIbYKrKlEEMMF/ExCiJEX=XMLPUANw`, "mysql-replica-01")
	c.Assert(err, IsNil)
	c.Assert(isS3, IsTrue)
	c.Assert(newURL, Equals, "s3://bucket4/prefix/path.mysql-replica-01?access-key=NXN7IPIOSAAKDEEOLMAF&secret-access-key=nREY/7Dt+PaIbYKrKlEEMMF/ExCiJEX=XMLPUANw")
}
