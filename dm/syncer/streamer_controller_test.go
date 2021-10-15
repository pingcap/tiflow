package syncer

import (
	"time"

	"github.com/go-mysql-org/go-mysql/replication"
	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
)

func (s *testSyncerSuite) TestIsConnectionRefusedError(c *C) {
	isConnRefusedErr := isConnectionRefusedError(nil)
	c.Assert(isConnRefusedErr, Equals, false)

	isConnRefusedErr = isConnectionRefusedError(errors.New("timeout"))
	c.Assert(isConnRefusedErr, Equals, false)

	isConnRefusedErr = isConnectionRefusedError(errors.New("connect: connection refused"))
	c.Assert(isConnRefusedErr, Equals, true)
}

func (s *testSyncerSuite) TestCanErrorRetry(c *C) {
	controller := NewStreamerController(replication.BinlogSyncerConfig{}, true, nil,
		LocalBinlog, "", nil)

	mockErr := errors.New("test")

	// local binlog puller can always retry
	for i := 0; i < 5; i++ {
		c.Assert(controller.CanRetry(mockErr), IsTrue)
	}

	origCfg := minErrorRetryInterval
	minErrorRetryInterval = 100 * time.Millisecond
	defer func() {
		minErrorRetryInterval = origCfg
	}()

	// test with remote binlog
	controller = NewStreamerController(replication.BinlogSyncerConfig{}, true, nil,
		RemoteBinlog, "", nil)

	c.Assert(controller.CanRetry(mockErr), IsTrue)
	c.Assert(controller.CanRetry(mockErr), IsFalse)
	time.Sleep(100 * time.Millisecond)
	c.Assert(controller.CanRetry(mockErr), IsTrue)
}
