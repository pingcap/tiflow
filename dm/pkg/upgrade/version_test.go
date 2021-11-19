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

package upgrade

import (
	. "github.com/pingcap/check"
)

func (t *testForEtcd) TestVersionJSON(c *C) {
	v1 := NewVersion(1, "v2.0.0")
	j, err := v1.toJSON()
	c.Assert(err, IsNil)
	c.Assert(j, Equals, `{"internal-no":1,"release-ver":"v2.0.0"}`)
	c.Assert(j, Equals, v1.String())

	v2, err := versionFromJSON(j)
	c.Assert(err, IsNil)
	c.Assert(v2, DeepEquals, v1)
}

func (t *testForEtcd) TestVersionFunc(c *C) {
	var ve Version
	c.Assert(ve.NotSet(), IsTrue)
	c.Assert(CurrentVersion.NotSet(), IsFalse)

	v1 := NewVersion(1, "v2.0.0")
	v2 := NewVersion(2, "v2.1.0")
	c.Assert(v1.Compare(v2), Equals, -1)
	c.Assert(v2.Compare(v1), Equals, 1)

	v11 := v1
	c.Assert(v11.Compare(v1), Equals, 0)

	// we only compare InternalNo now, if we should compare release version later, this should be fail.
	v12 := NewVersion(v1.InternalNo, "another-release-ver")
	c.Assert(v12.Compare(v1), Equals, 0)

	// MinVersion should < any current version.
	c.Assert(MinVersion.Compare(CurrentVersion), Equals, -1)
}

func (t *testForEtcd) TestVersionEtcd(c *C) {
	defer clearTestData(c)

	// try to get the version, but not exist.
	ver, rev1, err := GetVersion(etcdTestCli)
	c.Assert(err, IsNil)
	c.Assert(rev1, Greater, int64(0))
	c.Assert(ver.NotSet(), IsTrue)

	// put current version into etcd.
	rev2, err := PutVersion(etcdTestCli, CurrentVersion)
	c.Assert(err, IsNil)
	c.Assert(rev2, Greater, rev1)

	// get the version back.
	ver, rev3, err := GetVersion(etcdTestCli)
	c.Assert(err, IsNil)
	c.Assert(rev3, Equals, rev2)
	c.Assert(ver, DeepEquals, CurrentVersion)
}
