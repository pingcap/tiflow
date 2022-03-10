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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"

	"github.com/pingcap/tiflow/dm/pkg/log"
)

type testMCPSuite struct {
	suite.Suite
	mcp *ModificationCandidatePool
}

func (s *testMCPSuite) SetupSuite() {
	assert.Nil(s.T(), log.InitLogger(&log.Config{}))
}

func (s *testMCPSuite) SetupTest() {
	mcp := NewModificationCandidatePool()
	for i := 0; i < 4096; i++ {
		mcp.keyPool = append(mcp.keyPool, &UniqueKey{
			RowID: i,
			Value: map[string]interface{}{
				"id": i,
			},
		})
	}
	s.mcp = mcp
}

func (s *testMCPSuite) TestMCPNextUK() {
	for i := 0; i < 10; i++ {
		theUK := s.mcp.NextUK()
		s.T().Logf("next UK: %v", theUK)
	}
}

func (s *testMCPSuite) TestMCPAddDeleteBasic() {
	var (
		curPoolSize int
		err         error
	)
	curPoolSize = len(s.mcp.keyPool)
	for i := 0; i < 5; i++ {
		err = s.mcp.AddUK(&UniqueKey{
			RowID: -1,
			Value: map[string]interface{}{
				"id": rand.Int(),
			},
		})
		assert.Nil(s.T(), err)
		assert.Equal(s.T(), len(s.mcp.keyPool), curPoolSize+i+1, "key pool size is not equal")
		assert.Equal(s.T(), s.mcp.keyPool[curPoolSize+i].RowID, curPoolSize+i, "the new added UK's row ID is abnormal")
		s.T().Logf("new added UK: %v\n", s.mcp.keyPool[curPoolSize+i])
	}
	// test delete from bottom
	curPoolSize = len(s.mcp.keyPool)
	for i := 0; i < 5; i++ {
		err = s.mcp.DeleteUK(&UniqueKey{
			RowID: curPoolSize - i - 1,
		})
		assert.Nil(s.T(), err)
		assert.Equal(s.T(), len(s.mcp.keyPool), curPoolSize-i-1, "key pool size is not equal")
	}
	// test delete from top
	for i := 0; i < 5; i++ {
		err = s.mcp.DeleteUK(&UniqueKey{
			RowID: i,
		})
		assert.Nil(s.T(), err)
		assert.Equal(s.T(), s.mcp.keyPool[i].RowID, i, "the new added UK's row ID is abnormal")
		s.T().Logf("new UK after delete on the index %d: %v\n", i, s.mcp.keyPool[i])
	}
	// test delete at random position
	for i := 0; i < 5; i++ {
		theUK := s.mcp.NextUK()
		deleteRowID := theUK.RowID
		err = s.mcp.DeleteUK(theUK)
		assert.Nil(s.T(), err)
		assert.Equal(s.T(), s.mcp.keyPool[deleteRowID].RowID, deleteRowID, "the new added UK's row ID is abnormal")
		s.T().Logf("new UK after delete on the index %d: %v\n", deleteRowID, s.mcp.keyPool[deleteRowID])
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
				theUK := &UniqueKey{
					RowID: -1,
					Value: map[string]interface{}{
						"id": rand.Int(),
					},
				}
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
				theUK := s.mcp.NextUK()
				deletedRowID := theUK.RowID
				err = s.mcp.DeleteUK(theUK)
				if err != nil {
					return
				}
				s.T().Logf("deletedUK: %v\n", theUK)
				if theUK != nil {
					s.mcp.RLock()
					s.T().Logf("new position on UK: %v\n", s.mcp.keyPool[deletedRowID])
					s.mcp.RUnlock()
				}
			}
		}()
		return ch
	}()
	close(pendingCh)
	err1 := <-ch1
	err2 := <-ch2
	assert.Nil(s.T(), err1)
	assert.Nil(s.T(), err2)
	afterLen := s.mcp.Len()
	assert.Equal(s.T(), beforeLen, afterLen, "the key pool size has changed after the parallel modification")
}

func TestMCPSuite(t *testing.T) {
	suite.Run(t, &testMCPSuite{})
}
