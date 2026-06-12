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

func (t *testForEtcd) TestVersionJSON() {
	v1 := NewVersion(1, "v2.0.0")
	j, err := v1.toJSON()
	t.Require().NoError(err)
	t.Require().Equal(`{"internal-no":1,"release-ver":"v2.0.0"}`, j)
	t.Require().Equal(v1.String(), j)

	v2, err := versionFromJSON(j)
	t.Require().NoError(err)
	t.Require().Equal(v1, v2)
}

func (t *testForEtcd) TestVersionFunc() {
	var ve Version
	t.Require().True(ve.NotSet())
	t.Require().False(CurrentVersion.NotSet())

	v1 := NewVersion(1, "v2.0.0")
	v2 := NewVersion(2, "v2.1.0")
	t.Require().Equal(-1, v1.Compare(v2))
	t.Require().Equal(1, v2.Compare(v1))

	v11 := v1
	t.Require().Equal(0, v11.Compare(v1))

	// we only compare InternalNo now, if we should compare release version later, this should be fail.
	v12 := NewVersion(v1.InternalNo, "another-release-ver")
	t.Require().Equal(0, v12.Compare(v1))

	// MinVersion should < any current version.
	t.Require().Equal(-1, MinVersion.Compare(CurrentVersion))
}

func (t *testForEtcd) TestVersionEtcd() {
	defer clearTestData(t.T())

	// try to get the version, but not exist.
	ver, rev1, err := GetVersion(etcdTestCli)
	t.Require().NoError(err)
	t.Require().Greater(rev1, int64(0))
	t.Require().True(ver.NotSet())

	// put current version into etcd.
	rev2, err := PutVersion(etcdTestCli, CurrentVersion)
	t.Require().NoError(err)
	t.Require().Greater(rev2, rev1)

	// get the version back.
	ver, rev3, err := GetVersion(etcdTestCli)
	t.Require().NoError(err)
	t.Require().Equal(rev2, rev3)
	t.Require().Equal(CurrentVersion, ver)
}
