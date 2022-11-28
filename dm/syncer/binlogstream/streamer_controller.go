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
	"github.com/pingcap/tiflow/dm/pb"
	"github.com/pingcap/tiflow/dm/pkg/binlog"
	"github.com/pingcap/tiflow/dm/pkg/binlog/reader"
	tcontext "github.com/pingcap/tiflow/dm/pkg/context"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/dm/pkg/terror"
	"github.com/pingcap/tiflow/dm/pkg/utils"
	"github.com/pingcap/tiflow/dm/relay"
	"github.com/pingcap/tiflow/dm/syncer/dbconn"
	"go.uber.org/zap"
)

// streamGenerator provides the ability to generate reader.Streamer from
// specified location. Current implementation of reader.Streamer are MySQL binlog
// streamer, relay log streamer and mock streamer.
type streamGenerator interface {
	GenerateStreamFrom(location binlog.Location) (reader.Streamer, error)
}

type localBinlogReader struct {
	reader     *relay.BinlogReader
	EnableGTID bool
}

func (l *localBinlogReader) GenerateStreamFrom(location binlog.Location) (reader.Streamer, error) {
	if l.EnableGTID {
		return l.reader.StartSyncByGTID(location.GetGTID().Clone())
	}
	return l.reader.StartSyncByPos(location.Position)
}

type remoteBinlogReader struct {
	reader     *replication.BinlogSyncer
	tctx       *tcontext.Context
	flavor     string
	EnableGTID bool
}

func (r *remoteBinlogReader) GenerateStreamFrom(location binlog.Location) (reader.Streamer, error) {
	defer func() {
		lastSlaveConnectionID := r.reader.LastConnectionID()
		r.tctx.L().Info("last slave connection", zap.Uint32("connection ID", lastSlaveConnectionID))
	}()

	if r.EnableGTID {
		streamer, err := r.reader.StartSyncGTID(location.GetGTID().Clone())
		return streamer, terror.ErrSyncerUnitRemoteSteamerStartSync.Delegate(err)
	}

	// position's name may contain uuid, so need remove it
	adjustedPos := binlog.RemoveRelaySubDirSuffix(location.Position)
	streamer, err := r.reader.StartSync(adjustedPos)
	return streamer, terror.ErrSyncerUnitRemoteSteamerStartSync.Delegate(err)
}

type locationStream struct {
	stream reader.Streamer
	*locationRecorder
}

func newLocationStream(generator streamGenerator, location binlog.Location) (locationStream, error) {
	var ret locationStream

	// strip injected event suffix
	location.Suffix = 0
	s, err := generator.GenerateStreamFrom(location)
	if err != nil {
		return ret, err
	}
	ret.stream = s

	recorder := newLocationRecorder()
	recorder.reset(location)
	ret.locationRecorder = recorder
	return ret, nil
}

func (l locationStream) GetEvent(ctx context.Context) (*replication.BinlogEvent, error) {
	e, err := l.stream.GetEvent(ctx)
	if err != nil {
		return nil, err
	}

	failpoint.Inject("MakeFakeRotateEvent", func(val failpoint.Value) {
		ev, ok := e.Event.(*replication.RotateEvent)
		if ok {
			e.Header.LogPos = 0
			e.Header.Flags = replication.LOG_EVENT_ARTIFICIAL_F
			ev.NextLogName = []byte(val.(string))
			log.L().Info("MakeFakeRotateEvent", zap.String("fake file name", string(ev.NextLogName)))
		}
	})

	l.locationRecorder.update(e)
	return e, nil
}

// StreamerController controls the streamer for read binlog, include:
// 1. reset streamer to a binlog position or GTID
// 2. read next binlog event
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

	streamProducer streamGenerator
	upstream       locationStream

	*streamModifier
	// streamModifier will also modify locations so they'll be different from upstreamLocations.
	locations *locations

	// lastEventFromUpstream is the last event from upstream, and not sent to caller
	// yet. It should be set to nil after sent to caller.
	lastEventFromUpstream *replication.BinlogEvent

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
		locations:         &locations{},
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

	return c.upstream
}

func (c *StreamerController) resetReplicationSyncer(tctx *tcontext.Context, location binlog.Location) (err error) {
	uuidSameWithUpstream := true

	// close old streamProducer
	if c.streamProducer != nil {
		switch t := c.streamProducer.(type) {
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
			c.upstream, err = newLocationStream(c.streamProducer, location)
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
		c.streamProducer = &remoteBinlogReader{replication.NewBinlogSyncer(c.syncCfg), tctx, c.syncCfg.Flavor, c.enableGTID}
	} else {
		c.streamProducer = &localBinlogReader{c.relay.NewReader(tctx.L(), &relay.BinlogReaderConfig{RelayDir: c.localBinlogDir, Timezone: c.timezone, Flavor: c.syncCfg.Flavor, RowsEventDecodeFunc: c.syncCfg.RowsEventDecodeFunc}), c.enableGTID}
	}

	c.upstream, err = newLocationStream(c.streamProducer, location)
	if err == nil {
		c.streamModifier.reset(location)
		c.locations.reset(location)
		c.lastEventFromUpstream = nil
	}
	return err
}

// GetEvent returns binlog event from upstream binlog or streamModifier. It's not
// concurrent safe.
//
// After GetEvent returns an event, GetCurStartLocation, GetCurEndLocation, GetTxnEndLocation
// will return the corresponding locations of the event. The definition of 3 locations
// can be found in the comment of locations struct in binlog_locations.go .
//
// When return events from streamModifier, 3 locations are maintained as below:
//
//   - Inject
//     if we inject events [DDL1, DDL2] at (start) position 900, where start position
//     900 has Insert1 event whose LogPos (end position) is 1000, we should return
//     to caller like
//
//     1. DDL1, start (900, suffix 0) end (900, suffix 1)
//     2. DDL2, start (900, suffix 1) end (900, suffix 2)
//     3. Insert1, start (900, suffix 2) end (1000, suffix 0)
//
//     The DDLs are placed before DML because user may want to use Inject to change
//     table structure for DML.
//
//   - Replace
//     if we replace events [DDL1, DDL2] at (start) position 900, where start position
//     900 has DDL0 event whose LogPos (end position) is 1000, we should return to
//     caller like
//
//     1. DDL1, start (900, suffix 0) end (900, suffix 1)
//     2. DDL2, start (900, suffix 1) end (1000, suffix 0)
//
//   - Skip
//     the skipped event will still be sent to caller, with op = pb.ErrorOp_Skip,
//     to let caller track schema and save checkpoints.
func (c *StreamerController) GetEvent(tctx *tcontext.Context) (*replication.BinlogEvent, pb.ErrorOp, error) {
	event, suffix, op, err := c.getEvent(tctx)
	// if is local binlog but switch to remote on error, need to add uuid information in binlog's filename
	if err == nil {
		if ev, ok := event.Event.(*replication.RotateEvent); ok {
			// nolint:dogsled
			_, relaySubDirSuffix, _, _ := utils.SplitFilenameWithUUIDSuffix(string(ev.NextLogName))
			if relaySubDirSuffix != "" {
				c.relaySubDirSuffix = relaySubDirSuffix
			} else if c.relaySubDirSuffix != "" {
				filename, err2 := utils.ParseFilename(string(ev.NextLogName))
				if err2 != nil {
					return nil, pb.ErrorOp_InvalidErrorOp, terror.Annotate(err2, "fail to parse binlog file name from rotate event")
				}
				ev.NextLogName = []byte(utils.ConstructFilenameWithUUIDSuffix(filename, c.relaySubDirSuffix))
			}
		}
	}

	if err != nil {
		if err == context.Canceled || err == context.DeadlineExceeded {
			return nil, pb.ErrorOp_InvalidErrorOp, err
		}
		c.Lock()
		tctx.L().Error("meet error when get binlog event", zap.Error(err))
		c.meetError = true
		c.Unlock()
		return nil, pb.ErrorOp_InvalidErrorOp, err
	}

	if suffix != 0 {
		c.locations.curStartLocation = c.upstream.curStartLocation
		c.locations.curStartLocation.Suffix = suffix - 1
		c.locations.curEndLocation = c.upstream.curStartLocation
		c.locations.curEndLocation.Suffix = suffix
		// we only allow injecting DDL, so txnEndLocation is end location of every injected event
		c.locations.txnEndLocation = c.locations.curEndLocation
		return event, op, nil
	}

	if isDataEvent(event) {
		c.locations.curStartLocation = c.locations.curEndLocation
		c.locations.curEndLocation = c.upstream.curEndLocation
		c.locations.txnEndLocation = c.upstream.txnEndLocation
		return event, op, nil
	}

	c.locations.curStartLocation = c.locations.curEndLocation
	c.locations.curEndLocation.CopyWithoutSuffixFrom(c.upstream.curEndLocation)
	c.locations.txnEndLocation.CopyWithoutSuffixFrom(c.upstream.txnEndLocation)

	return event, op, nil
}

// getEvent gets event from upstream binlog or streamModifier. The maintaining of
// locations is on its caller.
func (c *StreamerController) getEvent(tctx *tcontext.Context) (
	event *replication.BinlogEvent,
	suffix int,
	op pb.ErrorOp,
	err error,
) {
	failpoint.Inject("SyncerGetEventError", func(_ failpoint.Value) {
		c.meetError = true
		tctx.L().Info("mock upstream instance restart", zap.String("failpoint", "SyncerGetEventError"))
		failpoint.Return(nil, 0, pb.ErrorOp_InvalidErrorOp, terror.ErrDBBadConn.Generate())
	})

	var status getEventFromFrontOpStatus

LOOP:
	for frontOp := c.streamModifier.front(); frontOp != nil; frontOp = c.streamModifier.front() {
		op = pb.ErrorOp_InvalidErrorOp
		suffix = 0

		if c.lastEventFromUpstream == nil {
			c.lastEventFromUpstream, err = c.upstream.GetEvent(tctx.Context())
			failpoint.Inject("GetEventError", func() {
				err = errors.New("go-mysql returned an error")
			})
			if err != nil {
				return
			}
		}

		// fake rotate. binlog recorder should handle it
		if c.lastEventFromUpstream.Header.LogPos == 0 {
			event = c.lastEventFromUpstream
			c.lastEventFromUpstream = nil
			return
		}

		startPos := mysql.Position{
			Name: c.upstream.curEndLocation.Position.Name,
			Pos:  c.lastEventFromUpstream.Header.LogPos - c.lastEventFromUpstream.Header.EventSize,
		}
		cmp := binlog.ComparePosition(startPos, frontOp.pos)
		switch cmp {
		// when upstream event is earlier than any injected op.
		case -1:
			event = c.lastEventFromUpstream
			c.lastEventFromUpstream = nil
			break LOOP
		// when upstream event is at the same location as injected op.
		case 0:
			// TODO: check it's DDL
			switch frontOp.op {
			case pb.ErrorOp_Skip:
				// skipped event and op should be sent to caller, to let schema
				// tracker and checkpoint work
				event = c.lastEventFromUpstream
				c.lastEventFromUpstream = nil
				c.streamModifier.next()
				op = pb.ErrorOp_Skip
				break LOOP
			case pb.ErrorOp_Replace:
				event, status = c.streamModifier.getEventFromFrontOp()
				// this op has been totally consumed, move to next op and also delete
				// lastEventFromUpstream because it's Replace.
				if status == eventsExhausted {
					c.lastEventFromUpstream = nil
					c.streamModifier.next()
					continue
				}

				// for last event in Replace, we change the LogPos and EventSize
				// to imitate the real event. otherwise, we use the LogPos from
				// startPosition, zero EventSize and increase suffix.
				if status == lastEvent {
					event.Header.LogPos = c.lastEventFromUpstream.Header.LogPos
					event.Header.EventSize = c.lastEventFromUpstream.Header.EventSize
					suffix = 0
				} else {
					event.Header.LogPos = startPos.Pos
					suffix = c.streamModifier.nextEventInOp
				}
				op = pb.ErrorOp_Replace
				break LOOP
			case pb.ErrorOp_Inject:
				event, status = c.streamModifier.getEventFromFrontOp()
				// this op has been totally consumed, move to next op and return
				// original upstream event
				if status == eventsExhausted {
					c.streamModifier.next()
					event = c.lastEventFromUpstream
					c.lastEventFromUpstream = nil
					break LOOP
				}

				event.Header.LogPos = startPos.Pos
				suffix = c.streamModifier.nextEventInOp
				op = pb.ErrorOp_Inject
				break LOOP
			default:
				c.logger.DPanic("invalid error handle op", zap.Stringer("op", frontOp.op))
			}
		// when upstream event is later than front op. Apart from Inject,
		// This may happen when user use handle-error but forget to specify source,
		// so all workers receive the handle-error command.
		case 1:
			switch frontOp.op {
			case pb.ErrorOp_Inject:
				event, status = c.streamModifier.getEventFromFrontOp()
				if status == eventsExhausted {
					c.streamModifier.next()
					continue
				}
				op = pb.ErrorOp_Inject
				break LOOP
			default:
				c.logger.Warn("mismatched handle op",
					zap.Stringer("op", frontOp.op),
					zap.Stringer("startPos", startPos),
					zap.Stringer("frontOp", frontOp.pos),
				)
				c.streamModifier.next()
			}
		}
	}

	if event != nil {
		return
	}

	// above loop will be terminated when streamModifier is empty. We still need
	// to send the last event from upstream.

	if c.lastEventFromUpstream != nil {
		event = c.lastEventFromUpstream
		c.lastEventFromUpstream = nil
		return
	}

	event, err = c.upstream.GetEvent(tctx.Context())
	failpoint.Inject("GetEventError", func() {
		err = errors.New("go-mysql returned an error")
	})
	// nolint:nakedret
	return
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

	if c.streamProducer != nil {
		switch r := c.streamProducer.(type) {
		case *remoteBinlogReader:
			// process remote binlog reader
			r.reader.Close()
		case *localBinlogReader:
			// process local binlog reader
			r.reader.Close()
		}
		c.streamProducer = nil
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
	_, uuidSuffix, _, err := utils.SplitFilenameWithUUIDSuffix(pos.Name)
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

func (c *StreamerController) GetCurStartLocation() binlog.Location {
	return c.locations.curStartLocation
}

func (c *StreamerController) GetCurEndLocation() binlog.Location {
	return c.locations.curEndLocation
}

func (c *StreamerController) GetTxnEndLocation() binlog.Location {
	return c.locations.txnEndLocation
}

// NewStreamerController4Test is used in tests.
func NewStreamerController4Test(
	streamerProducer streamGenerator,
	streamer reader.Streamer,
) *StreamerController {
	return &StreamerController{
		streamProducer: streamerProducer,
		upstream: locationStream{
			stream:           streamer,
			locationRecorder: newLocationRecorder(),
		},
		closed:         false,
		streamModifier: &streamModifier{logger: log.L()},
		locations:      &locations{},
	}
}
