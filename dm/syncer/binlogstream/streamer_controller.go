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

// Package binlogstream is used by syncer to read binlog.
// All information related to upstream binlog stream should be kept in this package,
// such as reset binlog to a location, maintain properties of the binlog event
// and stream, inject or delete binlog events with binlog stream, etc.
package binlogstream

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tiflow/dm/dm/pb"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"go.uber.org/zap"

	"github.com/pingcap/tiflow/dm/pkg/binlog"
	"github.com/pingcap/tiflow/dm/pkg/binlog/reader"
	tcontext "github.com/pingcap/tiflow/dm/pkg/context"
	"github.com/pingcap/tiflow/dm/pkg/terror"
	"github.com/pingcap/tiflow/dm/pkg/utils"
	"github.com/pingcap/tiflow/dm/relay"
	"github.com/pingcap/tiflow/dm/syncer/dbconn"
)

// the minimal interval to retry reset binlog streamer.
var minErrorRetryInterval = 1 * time.Minute

// BinlogType represents binlog type from syncer's view.
type BinlogType uint8

const (
	// RemoteBinlog means syncer is connected to MySQL and reading binlog.
	RemoteBinlog BinlogType = iota + 1
	// LocalBinlog means syncer is reading binlog from relay log.
	LocalBinlog
)

// RelayToBinlogType converts relay.Process to BinlogType.
func RelayToBinlogType(relay relay.Process) BinlogType {
	if relay != nil {
		return LocalBinlog
	}

	return RemoteBinlog
}

func (t BinlogType) String() string {
	switch t {
	case RemoteBinlog:
		return "remote"
	case LocalBinlog:
		return "local"
	default:
		return "unknown"
	}
}

// StreamerProducer provides the ability to generate binlog streamer by StartSync()
// but go-mysql StartSync() returns (struct, err) rather than (interface, err)
// And we can't simply use StartSync() method in SteamerProducer
// so use GenerateStreamer to wrap StartSync() method to make *BinlogSyncer and *BinlogReader in same interface
// For other implementations who implement StreamerProducer and Streamer can easily take place of Syncer.streamProducer
// For test is easy to mock.
type StreamerProducer interface {
	GenerateStreamer(location binlog.Location) (reader.Streamer, error)
}

// Read local relay log.
type localBinlogReader struct {
	reader     *relay.BinlogReader
	EnableGTID bool
}

func (l *localBinlogReader) GenerateStreamer(location binlog.Location) (reader.Streamer, error) {
	if l.EnableGTID {
		return l.reader.StartSyncByGTID(location.GetGTID().Clone())
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

func (r *remoteBinlogReader) GenerateStreamer(location binlog.Location) (reader.Streamer, error) {
	defer func() {
		lastSlaveConnectionID := r.reader.LastConnectionID()
		r.tctx.L().Info("last slave connection", zap.Uint32("connection ID", lastSlaveConnectionID))
	}()

	if r.EnableGTID {
		streamer, err := r.reader.StartSyncGTID(location.GetGTID().Clone())
		return streamer, terror.ErrSyncerUnitRemoteSteamerStartSync.Delegate(err)
	}

	// position's name may contain uuid, so need remove it
	adjustedPos := binlog.AdjustPosition(location.Position)
	streamer, err := r.reader.StartSync(adjustedPos)
	return streamer, terror.ErrSyncerUnitRemoteSteamerStartSync.Delegate(err)
}

// StreamerController controls the streamer for read binlog, include:
// 1. reset streamer to a binlog position or GTID
// 2. read next binlog event (TODO: and maintain the locations of the events)
// 3. transfer from local streamer to remote streamer.
type StreamerController struct {
	sync.RWMutex

	// the initial binlog type
	initBinlogType BinlogType

	// the current binlog type
	currentBinlogType BinlogType
	retryStrategy     retryStrategy

	syncCfg        replication.BinlogSyncerConfig
	enableGTID     bool
	localBinlogDir string
	timezone       *time.Location

	streamer         reader.Streamer
	streamerProducer StreamerProducer
	*streamModifier

	lastEvent      *replication.BinlogEvent
	currBinlogFile string // TODO: should be replaced by location recorder

	// meetError means meeting error when get binlog event
	// if binlogType is local and meetError is true, then need to create remote binlog stream
	meetError bool

	fromDB *dbconn.UpStreamConn

	relaySubDirSuffix string

	closed bool

	// whether the server id is updated
	serverIDUpdated bool
	relay           relay.Process
}

// NewStreamerController creates a new streamer controller.
func NewStreamerController(
	syncCfg replication.BinlogSyncerConfig,
	enableGTID bool,
	fromDB *dbconn.UpStreamConn,
	localBinlogDir string,
	timezone *time.Location,
	relay relay.Process,
	logger log.Logger,
) *StreamerController {
	var strategy retryStrategy = alwaysRetryStrategy{}
	binlogType := RelayToBinlogType(relay)
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
		relay:             relay,
		streamModifier:    newStreamModifier(logger),
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
		c.close()
		return err
	}

	return nil
}

// ResetReplicationSyncer reset the replication.
func (c *StreamerController) ResetReplicationSyncer(tctx *tcontext.Context, location binlog.Location) (err error) {
	c.Lock()
	defer c.Unlock()

	tctx.L().Info("reset replication syncer", zap.Stringer("location", location))
	return c.resetReplicationSyncer(tctx, location)
}

func (c *StreamerController) GetStreamer() reader.Streamer {
	c.Lock()
	defer c.Unlock()

	return c.streamer
}

func (c *StreamerController) resetReplicationSyncer(tctx *tcontext.Context, location binlog.Location) (err error) {
	uuidSameWithUpstream := true

	// close old streamerProducer
	if c.streamerProducer != nil {
		switch t := c.streamerProducer.(type) {
		case *remoteBinlogReader:
			// unclosed conn bug already fixed in go-mysql, https://github.com/go-mysql-org/go-mysql/pull/411
			t.reader.Close()
		case *localBinlogReader:
			// check the uuid before close
			ctx, cancel := context.WithTimeout(tctx.Ctx, utils.DefaultDBTimeout)
			defer cancel()
			uuidSameWithUpstream, err = c.checkUUIDSameWithUpstream(ctx, location.Position, t.reader.GetSubDirs())
			if err != nil {
				return err
			}
			t.reader.Close()
		default:
			// some other producers such as mockStreamerProducer, should not re-create
			c.streamer, err = c.streamerProducer.GenerateStreamer(location)
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
		c.streamerProducer = &localBinlogReader{c.relay.NewReader(tctx.L(), &relay.BinlogReaderConfig{RelayDir: c.localBinlogDir, Timezone: c.timezone, Flavor: c.syncCfg.Flavor}), c.enableGTID}
	}

	c.streamer, err = c.streamerProducer.GenerateStreamer(location)
	if err == nil {
		c.streamModifier.reset(location)
	}
	return err
}

var mockRestarted = false

// GetEvent returns binlog event from upstream binlog or streamModifier, should
// only have one thread call this function.
func (c *StreamerController) GetEvent(tctx *tcontext.Context) (event *replication.BinlogEvent, op pb.ErrorOp, err error) {
	failpoint.Inject("SyncerGetEventError", func(_ failpoint.Value) {
		if !mockRestarted {
			mockRestarted = true
			c.meetError = true
			tctx.L().Info("mock upstream instance restart", zap.String("failpoint", "SyncerGetEventError"))
			failpoint.Return(nil, pb.ErrorOp_InvalidErrorOp, terror.ErrDBBadConn.Generate())
		}
	})

	appendRelaySubDir := func() (*replication.BinlogEvent, pb.ErrorOp, error) {
		switch ev := event.Event.(type) {
		case *replication.RotateEvent:
			c.currBinlogFile = string(ev.NextLogName)
			// if is local binlog but switch to remote on error, need to add uuid information in binlog's name
			// nolint:dogsled
			_, relaySubDirSuffix, _, _ := binlog.SplitFilenameWithUUIDSuffix(string(ev.NextLogName))
			if relaySubDirSuffix != "" {
				c.relaySubDirSuffix = relaySubDirSuffix
			} else if c.relaySubDirSuffix != "" {
				filename, err := binlog.ParseFilename(string(ev.NextLogName))
				if err != nil {
					return nil, pb.ErrorOp_InvalidErrorOp, terror.Annotate(err, "fail to parse binlog file name from rotate event")
				}
				ev.NextLogName = []byte(binlog.ConstructFilenameWithUUIDSuffix(filename, c.relaySubDirSuffix))
				event.Event = ev
			}
		}
		return event, op, nil
	}

	c.RLock()
	streamer := c.streamer
	c.RUnlock()

FORLOOP:
	for frontOp := c.streamModifier.front(); frontOp != nil; {
		if c.lastEvent == nil {
			c.lastEvent, err = streamer.GetEvent(tctx.Context())
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

				return nil, pb.ErrorOp_InvalidErrorOp, err
			}
		}

		startPos := mysql.Position{
			Name: c.currBinlogFile,
			Pos:  c.lastEvent.Header.LogPos - c.lastEvent.Header.EventSize,
		}
		cmp := binlog.ComparePosition(startPos, frontOp.pos)
		switch cmp {
		case -1:
			event = c.lastEvent
			c.lastEvent = nil
			break FORLOOP
		case 0:
			// TODO: check it's DDL
			switch frontOp.op {
			case pb.ErrorOp_Skip:
				// skipped event and op should be sent to caller, to let schema
				// tracker and checkpoint work
				event = c.lastEvent
				c.lastEvent = nil
				c.streamModifier.next()
				op = pb.ErrorOp_Skip
				break FORLOOP
			case pb.ErrorOp_Replace:
				if c.streamModifier.nextEventInOp == len(frontOp.events) {
					c.lastEvent = nil
					c.streamModifier.next()
				}
				event = frontOp.events[c.streamModifier.nextEventInOp]
				c.streamModifier.nextEventInOp++
				op = pb.ErrorOp_Replace
				// TODO: currently we let caller to modify Suffix, after location recorder
				// binlog streamer itself can maintain it
				break FORLOOP
			case pb.ErrorOp_Inject:
				if c.streamModifier.nextEventInOp == len(frontOp.events) {
					c.streamModifier.next()
					event = c.lastEvent
					c.lastEvent = nil
					break FORLOOP
				}
				event = frontOp.events[c.streamModifier.nextEventInOp]
				c.streamModifier.nextEventInOp++
				// inject and replace seems same to caller to maintain Suffix, maybe unify them?
				op = pb.ErrorOp_Inject
				break FORLOOP
			default:
				c.logger.DPanic("invalid error handle op", zap.Stringer("op", frontOp.op))
			}
		case 1:
			switch frontOp.op {
			case pb.ErrorOp_Inject:
				if c.streamModifier.nextEventInOp == len(frontOp.events) {
					c.streamModifier.next()
					continue
				}
				event = frontOp.events[c.streamModifier.nextEventInOp]
				c.streamModifier.nextEventInOp++
				op = pb.ErrorOp_Inject
				break FORLOOP
			default:
				c.logger.DPanic("invalid error handle op", zap.Stringer("op", frontOp.op))
			}
		}
	}

	if event != nil {
		return appendRelaySubDir()
	}

	if c.lastEvent != nil {
		event = c.lastEvent
		c.lastEvent = nil
		return appendRelaySubDir()
	}

	event, err = streamer.GetEvent(tctx.Context())
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
		return nil, pb.ErrorOp_InvalidErrorOp, err
	}

	return appendRelaySubDir()
}

// Close closes streamer.
func (c *StreamerController) Close() {
	c.Lock()
	c.close()
	c.Unlock()
}

func (c *StreamerController) close() {
	if c.closed {
		return
	}

	if c.streamerProducer != nil {
		switch r := c.streamerProducer.(type) {
		case *remoteBinlogReader:
			// process remote binlog reader
			r.reader.Close()
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

// NewStreamerController4Test is used in tests.
func NewStreamerController4Test(
	streamerProducer StreamerProducer,
	streamer reader.Streamer,
) *StreamerController {
	return &StreamerController{
		streamerProducer: streamerProducer,
		streamer:         streamer,
		closed:           false,
		streamModifier:   &streamModifier{},
	}
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
