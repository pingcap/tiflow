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

package mcp

import (
	"math/rand"
	"sync"
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/pingcap/tiflow/dm/pkg/log"
)

type testMCPSuite struct {
	suite.Suite
	mcp *ModificationCandidatePool
}

func (s *testMCPSuite) SetupSuite() {
	s.Require().Nil(log.InitLogger(&log.Config{}))
}

func (s *testMCPSuite) SetupTest() {
	mcp := NewModificationCandidatePool(8192)
	for i := 0; i < 4096; i++ {
		mcp.keyPool = append(mcp.keyPool, &UniqueKey{
			rowID: i,
			value: map[string]interface{}{
				"id": i,
			},
		})
	}
	s.mcp = mcp
}

func (s *testMCPSuite) TestNextUK() {
	allHitRowIDs := map[int]int{}
	repeatCnt := 20
	for i := 0; i < repeatCnt; i++ {
		theUK := s.mcp.NextUK()
		s.Require().NotNil(theUK, "the picked UK should not be nil")
		theRowID := theUK.GetRowID()
		if _, ok := allHitRowIDs[theRowID]; !ok {
			allHitRowIDs[theRowID] = 0
		}
		allHitRowIDs[theRowID]++
		s.T().Logf("next UK: %v", theUK)
	}
	totalOccurredTimes := 0
	totalOccurredRowIDs := 0
	for _, times := range allHitRowIDs {
		totalOccurredRowIDs++
		totalOccurredTimes += times
	}
	s.Greater(totalOccurredRowIDs, 1, "there should be more than 1 occurred row IDs")
	s.Equal(repeatCnt, totalOccurredTimes, "total occurred UKs should equal the iteration count")
}

func (s *testMCPSuite) TestParallelNextUK() {
	allHitRowIDs := map[int]int{}
	repeatCnt := 20
	workerCnt := 5
	rowIDCh := make(chan int, workerCnt)
	var wg sync.WaitGroup
	wg.Add(workerCnt)
	for i := 0; i < workerCnt; i++ {
		go func() {
			defer wg.Done()
			for i := 0; i < repeatCnt; i++ {
				theUK := s.mcp.NextUK()
				if theUK != nil {
					rowIDCh <- theUK.GetRowID()
				}
			}
		}()
	}
	collectionFinishCh := func() <-chan struct{} {
		ch := make(chan struct{})
		go func() {
			defer close(ch)
			for rowID := range rowIDCh {
				if _, ok := allHitRowIDs[rowID]; !ok {
					allHitRowIDs[rowID] = 0
				}
				allHitRowIDs[rowID]++
			}
		}()
		return ch
	}()
	wg.Wait()
	close(rowIDCh)
	<-collectionFinishCh

	totalOccurredTimes := 0
	totalOccurredRowIDs := 0
	for _, times := range allHitRowIDs {
		totalOccurredRowIDs++
		totalOccurredTimes += times
	}
	s.Greater(totalOccurredRowIDs, 1, "there should be more than 1 occurred row IDs")
	s.Equal(repeatCnt*workerCnt, totalOccurredTimes, "total occurred UKs should equal the iteration count")
}

func (s *testMCPSuite) TestMCPAddDeleteBasic() {
	var (
		curPoolSize int
		repeatCnt   int
		err         error
	)
	curPoolSize = len(s.mcp.keyPool)
	startPoolSize := curPoolSize
	repeatCnt = 5
	for i := 0; i < repeatCnt; i++ {
		theUK := NewUniqueKey(-1, map[string]interface{}{
			"id": rand.Int(),
		})
		err = s.mcp.AddUK(theUK)
		s.Require().Nil(err)
		s.Equal(curPoolSize+i+1, len(s.mcp.keyPool), "key pool size is not equal")
		s.Equal(curPoolSize+i, s.mcp.keyPool[curPoolSize+i].GetRowID(), "the new added UK's row ID is abnormal")
		s.Equal(curPoolSize+i, theUK.GetRowID(), "the input UK's row ID is not changed")
		s.T().Logf("new added UK: %v\n", s.mcp.keyPool[curPoolSize+i])
	}
	// test delete from bottom
	curPoolSize = len(s.mcp.keyPool)
	for i := 0; i < repeatCnt; i++ {
		theUK := s.mcp.keyPool[curPoolSize-i-1]
		err = s.mcp.DeleteUK(NewUniqueKey(curPoolSize-i-1, nil))
		s.Require().Nil(err)
		s.Equal(-1, theUK.GetRowID(), "the deleted UK's row ID is not right")
		s.Equal(curPoolSize-i-1, len(s.mcp.keyPool), "key pool size is not equal")
	}
	curPoolSize = len(s.mcp.keyPool)
	s.Equal(startPoolSize, curPoolSize, "the MCP size is not right after adding & deleting")
	// test delete from top
	for i := 0; i < repeatCnt; i++ {
		theDelUK := s.mcp.keyPool[i]
		err = s.mcp.DeleteUK(NewUniqueKey(i, nil))
		s.Require().Nil(err)
		theSwappedUK := s.mcp.keyPool[i]
		swappedUKVal := theSwappedUK.GetValue()
		s.Equal(i, theSwappedUK.GetRowID(), "the swapped UK's row ID is abnormal")
		s.Equal(curPoolSize-i-1, swappedUKVal["id"], "the swapped UK's value is abnormal")
		s.Equal(-1, theDelUK.GetRowID(), "the deleted UK's row ID is not right")
		s.T().Logf("new UK after delete on the index %d: %v\n", i, s.mcp.keyPool[i])
	}
	curPoolSize = len(s.mcp.keyPool)
	// test delete at random position
	for i := 0; i < repeatCnt; i++ {
		theDelUK := s.mcp.NextUK()
		deleteRowID := theDelUK.GetRowID()
		err = s.mcp.DeleteUK(theDelUK)
		s.Require().Nil(err)
		theSwappedUK := s.mcp.keyPool[deleteRowID]
		swappedUKVal := theSwappedUK.GetValue()
		s.Equal(deleteRowID, theSwappedUK.GetRowID(), "the swapped UK's row ID is abnormal")
		s.Equal(-1, theDelUK.GetRowID(), "the deleted UK's row ID is not right")
		s.Equal(curPoolSize-i-1, swappedUKVal["id"], "the swapped UK's value is abnormal")
		s.T().Logf("new UK after delete on the index %d: %v\n", deleteRowID, s.mcp.keyPool[deleteRowID])
	}
	// check whether all the row ID is right
	curPoolSize = len(s.mcp.keyPool)
	for i := 0; i < curPoolSize; i++ {
		theUK := s.mcp.keyPool[i]
		s.Require().Equal(i, theUK.GetRowID(), "this UK element in the MCP has a wrong row ID")
	}
}

func (s *testMCPSuite) TestMCPAddDeleteInParallel() {
	beforeLen := s.mcp.Len()
	pendingCh := make(chan struct{})
	ch1 := func() <-chan error {
		ch := make(chan error)
		go func() {
			var err error
			<-pendingCh
			defer func() {
				ch <- err
			}()
			for i := 0; i < 5; i++ {
				theUK := NewUniqueKey(-1, map[string]interface{}{
					"id": rand.Int(),
				})
				err = s.mcp.AddUK(theUK)
				if err != nil {
					return
				}
				s.T().Logf("new added UK: %v\n", theUK)
			}
		}()
		return ch
	}()
	ch2 := func() <-chan error {
		ch := make(chan error)
		go func() {
			var err error
			<-pendingCh
			defer func() {
				ch <- err
			}()
			for i := 0; i < 5; i++ {
				theDelUK := s.mcp.NextUK()
				deletedRowID := theDelUK.rowID
				err = s.mcp.DeleteUK(theDelUK)
				if err != nil {
					return
				}
				s.mcp.RLock()
				theSwappedUK := s.mcp.keyPool[deletedRowID]
				s.mcp.RUnlock()
				s.T().Logf("deletedUK: %v, swapped UK: %v\n", theDelUK, theSwappedUK)
			}
		}()
		return ch
	}()
	close(pendingCh)
	err1 := <-ch1
	err2 := <-ch2
	s.Require().Nil(err1)
	s.Require().Nil(err2)
	afterLen := s.mcp.Len()
	s.Equal(beforeLen, afterLen, "the key pool size has changed after the parallel modification")
}

func TestMCPSuite(t *testing.T) {
	suite.Run(t, &testMCPSuite{})
}
