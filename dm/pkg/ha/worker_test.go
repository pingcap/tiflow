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

package ha

func (s *testForEtcd) TestWorkerInfoJSON() {
	i1 := NewWorkerInfo("dm-worker-1", "192.168.0.100:8262")

	j, err := i1.toJSON()
	s.Require().NoError(err)
	s.Require().Equal(`{"name":"dm-worker-1","addr":"192.168.0.100:8262"}`, j)
	s.Require().Equal(i1.String(), j)

	i2, err := workerInfoFromJSON(j)
	s.Require().NoError(err)
	s.Require().Equal(i1, i2)
}

func (s *testForEtcd) TestWorkerInfoEtcd() {
	defer clearTestInfoOperation(s.T())

	var (
		worker1 = "dm-worker-1"
		worker2 = "dm-worker-2"
		info1   = NewWorkerInfo(worker1, "192.168.0.100:8262")
		info2   = NewWorkerInfo(worker2, "192.168.0.101:8262")
	)

	// get without info.
	ifm, _, err := GetAllWorkerInfo(etcdTestCli)
	s.Require().NoError(err)
	s.Require().Len(ifm, 0)

	// put two info.
	rev1, err := PutWorkerInfo(etcdTestCli, info1)
	s.Require().NoError(err)
	s.Require().Greater(rev1, int64(0))
	rev2, err := PutWorkerInfo(etcdTestCli, info2)
	s.Require().NoError(err)
	s.Require().Greater(rev2, rev1)

	// get again, with two info.
	ifm, rev3, err := GetAllWorkerInfo(etcdTestCli)
	s.Require().NoError(err)
	s.Require().Equal(rev2, rev3)
	s.Require().Len(ifm, 2)
	s.Require().Equal(info1, ifm[worker1])
	s.Require().Equal(info2, ifm[worker2])

	// delete info1.
	rev4, err := DeleteWorkerInfoRelayConfig(etcdTestCli, worker1)
	s.Require().NoError(err)
	s.Require().Greater(rev4, rev3)

	// get again, with only one info.
	ifm, rev5, err := GetAllWorkerInfo(etcdTestCli)
	s.Require().NoError(err)
	s.Require().Equal(rev4, rev5)
	s.Require().Len(ifm, 1)
	s.Require().Equal(info2, ifm[worker2])
}
