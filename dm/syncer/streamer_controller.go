// Copyright 2019 PingCAP, Inc.
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

package syncer

import (
	"context"
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/pingcap/failpoint"
	"go.uber.org/zap"

	"github.com/pingcap/dm/pkg/binlog"
	"github.com/pingcap/dm/pkg/binlog/common"
	tcontext "github.com/pingcap/dm/pkg/context"
	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/dm/pkg/retry"
	"github.com/pingcap/dm/pkg/streamer"
	"github.com/pingcap/dm/pkg/terror"
	"github.com/pingcap/dm/pkg/utils"
	"github.com/pingcap/dm/syncer/dbconn"
)

// the minimal interval to retry reset binlog streamer.
var minErrorRetryInterval = 1 * time.Minute

// StreamerProducer provides the ability to generate binlog streamer by StartSync()
// but go-mysql StartSync() returns (struct, err) rather than (interface, err)
// And we can't simplely use StartSync() method in SteamerProducer
// so use generateStreamer to wrap StartSync() method to make *BinlogSyncer and *BinlogReader in same interface
// For other implementations who implement StreamerProducer and Streamer can easily take place of Syncer.streamProducer
// For test is easy to mock.
type StreamerProducer interface {
	generateStreamer(location binlog.Location) (streamer.Streamer, error)
}

// Read local relay log.
type localBinlogReader struct {
	reader     *streamer.BinlogReader
	EnableGTID bool
}

func (l *localBinlogReader) generateStreamer(location binlog.Location) (streamer.Streamer, error) {
	if l.EnableGTID {
		return l.reader.StartSyncByGTID(location.GetGTID().Origin().Clone())
	}
	return l.reader.StartSyncByPos(location.Position)
}

// Read remote binlog.
type remoteBinlogReader struct {
	reader     *replication.BinlogSyncer
	tctx       *tcontext.Context
	flavor     string
	EnableGTID bool
}

func (r *remoteBinlogReader) generateStreamer(location binlog.Location) (streamer.Streamer, error) {
	defer func() {
		lastSlaveConnectionID := r.reader.LastConnectionID()
		r.tctx.L().Info("last slave connection", zap.Uint32("connection ID", lastSlaveConnectionID))
	}()

	if r.EnableGTID {
		streamer, err := r.reader.StartSyncGTID(location.GetGTID().Origin().Clone())
		return streamer, terror.ErrSyncerUnitRemoteSteamerStartSync.Delegate(err)
	}

	// position's name may contain uuid, so need remove it
	adjustedPos := binlog.AdjustPosition(location.Position)
	streamer, err := r.reader.StartSync(adjustedPos)
	return streamer, terror.ErrSyncerUnitRemoteSteamerStartSync.Delegate(err)
}

// StreamerController controls the streamer for read binlog, include:
// 1. reopen streamer
// 2. redirect binlog position or gtid
// 3. transfor from local streamer to remote streamer.
type StreamerController struct {
	sync.RWMutex

	// the initital binlog type
	initBinlogType BinlogType

	// the current binlog type
	currentBinlogType BinlogType
	retryStrategy     retryStrategy

	syncCfg        replication.BinlogSyncerConfig
	enableGTID     bool
	localBinlogDir string
	timezone       *time.Location

	streamer         streamer.Streamer
	streamerProducer StreamerProducer

	// meetError means meeting error when get binlog event
	// if binlogType is local and meetError is true, then need to create remote binlog stream
	meetError bool

	fromDB *dbconn.UpStreamConn

	uuidSuffix string

	closed bool

	// whether the server id is updated
	serverIDUpdated bool
}

// NewStreamerController creates a new streamer controller.
func NewStreamerController(
	syncCfg replication.BinlogSyncerConfig,
	enableGTID bool,
	fromDB *dbconn.UpStreamConn,
	binlogType BinlogType,
	localBinlogDir string,
	timezone *time.Location,
) *StreamerController {
	var strategy retryStrategy = alwaysRetryStrategy{}
	if binlogType != LocalBinlog {
		strategy = &maxIntervalRetryStrategy{
			interval: minErrorRetryInterval,
		}
	}
	// let local binlog also return error to avoid infinity loop
	failpoint.Inject("GetEventError", func() {
		strategy = &maxIntervalRetryStrategy{
			interval: minErrorRetryInterval,
		}
	})
	streamerController := &StreamerController{
		initBinlogType:    binlogType,
		currentBinlogType: binlogType,
		retryStrategy:     strategy,
		syncCfg:           syncCfg,
		enableGTID:        enableGTID,
		localBinlogDir:    localBinlogDir,
		timezone:          timezone,
		fromDB:            fromDB,
		closed:            true,
	}

	return streamerController
}

// Start starts streamer controller.
func (c *StreamerController) Start(tctx *tcontext.Context, location binlog.Location) error {
	c.Lock()
	defer c.Unlock()

	c.meetError = false
	c.closed = false
	c.currentBinlogType = c.initBinlogType

	var err error
	if c.serverIDUpdated {
		err = c.resetReplicationSyncer(tctx, location)
	} else {
		err = c.updateServerIDAndResetReplication(tctx, location)
	}
	if err != nil {
		c.close(tctx)
		return err
	}

	return nil
}

// ResetReplicationSyncer reset the replication.
func (c *StreamerController) ResetReplicationSyncer(tctx *tcontext.Context, location binlog.Location) (err error) {
	c.Lock()
	defer c.Unlock()

	return c.resetReplicationSyncer(tctx, location)
}

func (c *StreamerController) resetReplicationSyncer(tctx *tcontext.Context, location binlog.Location) (err error) {
	uuidSameWithUpstream := true

	// close old streamerProducer
	if c.streamerProducer != nil {
		switch t := c.streamerProducer.(type) {
		case *remoteBinlogReader:
			err2 := c.closeBinlogSyncer(tctx, t.reader)
			if err2 != nil {
				tctx.L().Warn("fail to close remote binlog reader", zap.Error(err))
			}
		case *localBinlogReader:
			// check the uuid before close
			ctx, cancel := context.WithTimeout(tctx.Ctx, utils.DefaultDBTimeout)
			defer cancel()
			uuidSameWithUpstream, err = c.checkUUIDSameWithUpstream(ctx, location.Position, t.reader.GetUUIDs())
			if err != nil {
				return err
			}
			t.reader.Close()
		default:
			// some other producers such as mockStreamerProducer, should not re-create
			c.streamer, err = c.streamerProducer.generateStreamer(location)
			return err
		}
	}

	if c.currentBinlogType == LocalBinlog && c.meetError {
		// meetError is true means meets error when get binlog event, in this case use remote binlog as default
		if !uuidSameWithUpstream {
			// if the binlog position's uuid is different from the upstream, can not switch to remote binlog
			tctx.L().Warn("may switch master in upstream, so can not switch local to remote")
		} else {
			c.currentBinlogType = RemoteBinlog
			c.retryStrategy = &maxIntervalRetryStrategy{interval: minErrorRetryInterval}
			tctx.L().Warn("meet error when read from local binlog, will switch to remote binlog")
		}
	}

	if c.currentBinlogType == RemoteBinlog {
		c.streamerProducer = &remoteBinlogReader{replication.NewBinlogSyncer(c.syncCfg), tctx, c.syncCfg.Flavor, c.enableGTID}
	} else {
		c.streamerProducer = &localBinlogReader{streamer.NewBinlogReader(tctx.L(), &streamer.BinlogReaderConfig{RelayDir: c.localBinlogDir, Timezone: c.timezone, Flavor: c.syncCfg.Flavor}), c.enableGTID}
	}

	c.streamer, err = c.streamerProducer.generateStreamer(location)
	return err
}

// RedirectStreamer redirects the streamer's begin position or gtid.
func (c *StreamerController) RedirectStreamer(tctx *tcontext.Context, location binlog.Location) error {
	c.Lock()
	defer c.Unlock()

	tctx.L().Info("redirect streamer", zap.Stringer("location", location))
	return c.resetReplicationSyncer(tctx, location)
}

var mockRestarted = false

// GetEvent returns binlog event, should only have one thread call this function.
func (c *StreamerController) GetEvent(tctx *tcontext.Context) (event *replication.BinlogEvent, err error) {
	ctx, cancel := context.WithTimeout(tctx.Context(), common.SlaveReadTimeout)
	failpoint.Inject("SyncerEventTimeout", func(val failpoint.Value) {
		if seconds, ok := val.(int); ok {
			cancel()
			ctx, cancel = context.WithTimeout(tctx.Context(), time.Duration(seconds)*time.Second)
			tctx.L().Info("set fetch binlog event timeout", zap.String("failpoint", "SyncerEventTimeout"), zap.Int("value", seconds))
		}
	})

	failpoint.Inject("SyncerGetEventError", func(_ failpoint.Value) {
		if !mockRestarted {
			mockRestarted = true
			c.meetError = true
			tctx.L().Info("mock upstream instance restart", zap.String("failpoint", "SyncerGetEventError"))
			failpoint.Return(nil, terror.ErrDBBadConn.Generate())
		}
	})

	c.RLock()
	streamer := c.streamer
	c.RUnlock()

	event, err = streamer.GetEvent(ctx)
	cancel()
	failpoint.Inject("GetEventError", func() {
		err = errors.New("go-mysql returned an error")
	})
	if err != nil {
		if err != context.Canceled && err != context.DeadlineExceeded {
			c.Lock()
			tctx.L().Error("meet error when get binlog event", zap.Error(err))
			c.meetError = true
			c.Unlock()
		}

		return nil, err
	}

	switch ev := event.Event.(type) {
	case *replication.RotateEvent:
		// if is local binlog, binlog's name contain uuid information, need to save it
		// if is remote binlog, need to add uuid information in binlog's name
		c.Lock()
		containUUID := c.setUUIDIfExists(string(ev.NextLogName))
		uuidSuffix := c.uuidSuffix
		c.Unlock()

		if !containUUID {
			if len(uuidSuffix) != 0 {
				filename, err := binlog.ParseFilename(string(ev.NextLogName))
				if err != nil {
					return nil, terror.Annotate(err, "fail to parse binlog file name from rotate event")
				}
				ev.NextLogName = []byte(binlog.ConstructFilenameWithUUIDSuffix(filename, uuidSuffix))
				event.Event = ev
			}
		}
	default:
	}

	return event, nil
}

// ReopenWithRetry reopens streamer with retry.
func (c *StreamerController) ReopenWithRetry(tctx *tcontext.Context, location binlog.Location) error {
	c.Lock()
	defer c.Unlock()

	var err error
	for i := 0; i < maxRetryCount; i++ {
		err = c.resetReplicationSyncer(tctx, location)
		if err == nil {
			return nil
		}
		if retry.IsConnectionError(err) {
			tctx.L().Info("fail to retry open binlog streamer", log.ShortError(err))
			time.Sleep(retryTimeout)
			continue
		}
		break
	}
	return err
}

func (c *StreamerController) closeBinlogSyncer(logtctx *tcontext.Context, binlogSyncer *replication.BinlogSyncer) error {
	if binlogSyncer == nil {
		return nil
	}

	lastSlaveConnectionID := binlogSyncer.LastConnectionID()
	defer binlogSyncer.Close()
	if lastSlaveConnectionID > 0 {
		// try to KILL the conn in default timeout, but it's not a big problem even failed.
		ctx, cancel := context.WithTimeout(context.Background(), utils.DefaultDBTimeout)
		defer cancel()
		err := c.fromDB.KillConn(ctx, lastSlaveConnectionID)
		if err != nil {
			logtctx.L().Error("fail to kill last connection", zap.Uint32("connection ID", lastSlaveConnectionID), log.ShortError(err))
			if !utils.IsNoSuchThreadError(err) {
				return err
			}
		}
	}
	return nil
}

// Close closes streamer.
func (c *StreamerController) Close(tctx *tcontext.Context) {
	c.Lock()
	c.close(tctx)
	c.Unlock()
}

func (c *StreamerController) close(tctx *tcontext.Context) {
	if c.closed {
		return
	}

	if c.streamerProducer != nil {
		switch r := c.streamerProducer.(type) {
		case *remoteBinlogReader:
			// process remote binlog reader
			err := c.closeBinlogSyncer(tctx, r.reader)
			if err != nil {
				tctx.L().Warn("fail to close remote binlog reader", zap.Error(err))
			}
		case *localBinlogReader:
			// process local binlog reader
			r.reader.Close()
		}
		c.streamerProducer = nil
	}

	c.closed = true
}

// IsClosed returns whether streamer controller is closed.
func (c *StreamerController) IsClosed() bool {
	c.RLock()
	defer c.RUnlock()

	return c.closed
}

func (c *StreamerController) setUUIDIfExists(filename string) bool {
	_, uuidSuffix, _, err := binlog.SplitFilenameWithUUIDSuffix(filename)
	if err != nil {
		// don't contain uuid in position's name
		return false
	}

	c.uuidSuffix = uuidSuffix
	return true
}

// UpdateSyncCfg updates sync config and fromDB.
func (c *StreamerController) UpdateSyncCfg(syncCfg replication.BinlogSyncerConfig, fromDB *dbconn.UpStreamConn) {
	c.Lock()
	c.fromDB = fromDB
	c.syncCfg = syncCfg
	c.Unlock()
}

// check whether the uuid in binlog position's name is same with upstream.
func (c *StreamerController) checkUUIDSameWithUpstream(ctx context.Context, pos mysql.Position, uuids []string) (bool, error) {
	_, uuidSuffix, _, err := binlog.SplitFilenameWithUUIDSuffix(pos.Name)
	if err != nil {
		// don't contain uuid in position's name
		// nolint:nilerr
		return true, nil
	}
	uuid := utils.GetUUIDBySuffix(uuids, uuidSuffix)

	upstreamUUID, err := utils.GetServerUUID(ctx, c.fromDB.BaseDB.DB, c.syncCfg.Flavor)
	if err != nil {
		return false, terror.Annotate(err, "streamer controller check upstream uuid failed")
	}

	return uuid == upstreamUUID, nil
}

// GetBinlogType returns the binlog type used now.
func (c *StreamerController) GetBinlogType() BinlogType {
	c.RLock()
	defer c.RUnlock()
	return c.currentBinlogType
}

// CanRetry returns true if can switch from local to remote and retry again.
func (c *StreamerController) CanRetry(err error) bool {
	c.RLock()
	defer c.RUnlock()

	return c.retryStrategy.CanRetry(err)
}

func (c *StreamerController) updateServerID(tctx *tcontext.Context) error {
	randomServerID, err := utils.GetRandomServerID(tctx.Context(), c.fromDB.BaseDB.DB)
	if err != nil {
		// should never happened unless the master has too many slave
		return terror.Annotate(err, "fail to get random server id for streamer controller")
	}

	c.syncCfg.ServerID = randomServerID
	c.serverIDUpdated = true
	return nil
}

// UpdateServerIDAndResetReplication updates the server id and reset replication.
func (c *StreamerController) UpdateServerIDAndResetReplication(tctx *tcontext.Context, location binlog.Location) error {
	c.Lock()
	defer c.Unlock()

	return c.updateServerIDAndResetReplication(tctx, location)
}

func (c *StreamerController) updateServerIDAndResetReplication(tctx *tcontext.Context, location binlog.Location) error {
	err := c.updateServerID(tctx)
	if err != nil {
		return err
	}

	err = c.resetReplicationSyncer(tctx, location)
	if err != nil {
		return err
	}

	return nil
}

func isDuplicateServerIDError(err error) bool {
	if err == nil {
		return false
	}

	return strings.Contains(err.Error(), "A slave with the same server_uuid/server_id as this slave has connected to the master")
}

func isConnectionRefusedError(err error) bool {
	if err == nil {
		return false
	}

	return strings.Contains(err.Error(), "connect: connection refused")
}

// retryStrategy.
type retryStrategy interface {
	CanRetry(error) bool
}

type alwaysRetryStrategy struct{}

func (s alwaysRetryStrategy) CanRetry(error) bool {
	return true
}

// maxIntervalRetryStrategy allows retry when the retry interval is greater than the set interval.
type maxIntervalRetryStrategy struct {
	interval    time.Duration
	lastErr     error
	lastErrTime time.Time
}

func (s *maxIntervalRetryStrategy) CanRetry(err error) bool {
	if err == nil {
		return true
	}

	now := time.Now()
	lastErrTime := s.lastErrTime
	s.lastErrTime = now
	s.lastErr = err
	return lastErrTime.Add(s.interval).Before(now)
}
