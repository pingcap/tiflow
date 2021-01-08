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

package processor

/*

package main

import (
	"context"
	"time"

	"github.com/pingcap/log"

	"github.com/pingcap/errors"
	"github.com/pingcap/ticdc/cdc/kv"
	"github.com/pingcap/ticdc/pkg/etcd"
	"github.com/pingcap/ticdc/pkg/orchestrator"
	"github.com/prometheus/client_golang/prometheus"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/pkg/logutil"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
)

type cdcMonitor struct {
	etcdCli    *etcd.Client
	etcdWorker *orchestrator.EtcdWorker
	reactor    *cdcMonitReactor
}

func newCDCMonitor(ctx context.Context, pd string) (*cdcMonitor, error) {
	logConfig := logutil.DefaultZapLoggerConfig
	logConfig.Level = zap.NewAtomicLevelAt(zapcore.ErrorLevel)

	etcdCli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{pd},
		TLS:         nil,
		Context:     ctx,
		LogConfig:   &logConfig,
		DialTimeout: 5 * time.Second,
		DialOptions: []grpc.DialOption{
			grpc.WithInsecure(),
			grpc.WithBlock(),
			grpc.WithConnectParams(grpc.ConnectParams{
				Backoff: backoff.Config{
					BaseDelay:  time.Second,
					Multiplier: 1.1,
					Jitter:     0.1,
					MaxDelay:   3 * time.Second,
				},
				MinConnectTimeout: 3 * time.Second,
			}),
		},
	})
	if err != nil {
		return nil, errors.Trace(err)
	}

	wrappedCli := etcd.Wrap(etcdCli, map[string]prometheus.Counter{})
	reactor := &cdcMonitReactor{}
	initState := newCDCReactorState()
	etcdWorker, err := orchestrator.NewEtcdWorker(wrappedCli, kv.EtcdKeyBase, reactor, initState)
	if err != nil {
		return nil, errors.Trace(err)
	}

	ret := &cdcMonitor{
		etcdCli:    wrappedCli,
		etcdWorker: etcdWorker,
		reactor:    reactor,
	}

	return ret, nil
}

func (m *cdcMonitor) run(ctx context.Context) error {
	log.Debug("start running cdcMonitor")
	err := m.etcdWorker.Run(ctx, 200*time.Millisecond)
	log.Error("etcdWorker exited: test-case-failed", zap.Error(err))
	log.Info("CDC state", zap.Reflect("state", m.reactor.state))
	return err
}

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

package main

import (
	"context"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/pkg/orchestrator"
	"go.uber.org/zap"
)

type cdcMonitReactor struct {
	state *cdcReactorState
}

func (r *cdcMonitReactor) Tick(_ context.Context, state orchestrator.ReactorState) (orchestrator.ReactorState, error) {
	r.state = state.(*cdcReactorState)

	err := r.verifyTs()
	if err != nil {
		log.Error("Verifying Ts failed", zap.Error(err))
		return r.state, err
	}

	err = r.verifyStartTs()
	if err != nil {
		log.Error("Verifying startTs failed", zap.Error(err))
		return r.state, err
	}

	return r.state, nil
}

func (r *cdcMonitReactor) verifyTs() error {
	for changfeedID, positions := range r.state.TaskPositions {
		status, ok := r.state.ChangefeedStatuses[changfeedID]
		if !ok {
			log.Warn("changefeed status not found", zap.String("cfid", changfeedID))
			return nil
		}

		actualCheckpointTs := status.CheckpointTs

		for captureID, position := range positions {
			if position.CheckPointTs < actualCheckpointTs {
				return errors.Errorf("checkpointTs too large, globalCkpt = %d, localCkpt = %d, capture = %s, cfid = %s",
					actualCheckpointTs, position.CheckPointTs, captureID, changfeedID)
			}
		}
	}

	return nil
}

func (r *cdcMonitReactor) verifyStartTs() error {
	for changfeedID, statuses := range r.state.TaskStatuses {
		cStatus, ok := r.state.ChangefeedStatuses[changfeedID]
		if !ok {
			log.Warn("changefeed status not found", zap.String("cfid", changfeedID))
			return nil
		}

		actualCheckpointTs := cStatus.CheckpointTs

		for captureID, status := range statuses {
			for tableID, operation := range status.Operation {
				if operation.Status != model.OperFinished && !operation.Delete {
					startTs := status.Tables[tableID].StartTs
					if startTs < actualCheckpointTs {
						return errors.Errorf("startTs too small, globalCkpt = %d, startTs = %d, table = %d, capture = %s, cfid = %s",
							actualCheckpointTs, startTs, tableID, captureID, changfeedID)
					}
				}
			}
		}
	}

	return nil
}

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

package main

import (
	"encoding/json"
	"regexp"

	"go.uber.org/zap"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cdc/kv"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/pkg/orchestrator"
	"github.com/pingcap/ticdc/pkg/orchestrator/util"
)

type cdcReactorState struct {
	Owner              model.CaptureID
	Captures           map[model.CaptureID]*model.CaptureInfo
	ChangefeedStatuses map[model.ChangeFeedID]*model.ChangeFeedStatus
	TaskPositions      map[model.ChangeFeedID]map[model.CaptureID]*model.TaskPosition
	TaskStatuses       map[model.ChangeFeedID]map[model.CaptureID]*model.TaskStatus
}

var (
	captureRegex    = regexp.MustCompile(regexp.QuoteMeta(kv.CaptureInfoKeyPrefix) + "/(.+)")
	changefeedRegex = regexp.MustCompile(regexp.QuoteMeta(kv.JobKeyPrefix) + "/(.+)")
	positionRegex   = regexp.MustCompile(regexp.QuoteMeta(kv.TaskPositionKeyPrefix) + "/(.+?)/(.+)")
	statusRegex     = regexp.MustCompile(regexp.QuoteMeta(kv.TaskStatusKeyPrefix) + "/(.+?)/(.+)")
)

func newCDCReactorState() *cdcReactorState {
	return &cdcReactorState{
		Captures:           make(map[model.CaptureID]*model.CaptureInfo),
		ChangefeedStatuses: make(map[model.ChangeFeedID]*model.ChangeFeedStatus),
		TaskPositions:      make(map[model.ChangeFeedID]map[model.CaptureID]*model.TaskPosition),
		TaskStatuses:       make(map[model.ChangeFeedID]map[model.CaptureID]*model.TaskStatus),
	}
}

func (s *cdcReactorState) Update(key util.EtcdKey, value []byte, isInit bool) error {
	if key.String() == kv.CaptureOwnerKey {
		if value == nil {
			log.Info("Owner lost", zap.String("old-owner", s.Owner))
			return nil
		}

		log.Info("Owner updated", zap.String("old-owner", s.Owner),
			zap.ByteString("new-owner", value))
		s.Owner = string(value)
		return nil
	}

	if matches := captureRegex.FindSubmatch(key.Bytes()); matches != nil {
		captureID := string(matches[1])

		if value == nil {
			log.Info("Capture deleted",
				zap.String("captureID", captureID),
				zap.Reflect("old-capture", s.Captures[captureID]))

			delete(s.Captures, captureID)
			return nil
		}

		var newCaptureInfo model.CaptureInfo
		err := json.Unmarshal(value, &newCaptureInfo)
		if err != nil {
			return errors.Trace(err)
		}

		if oldCaptureInfo, ok := s.Captures[captureID]; ok {
			log.Info("Capture updated",
				zap.String("captureID", captureID),
				zap.Reflect("old-capture", oldCaptureInfo),
				zap.Reflect("new-capture", newCaptureInfo))
		} else {
			log.Info("Capture added",
				zap.String("captureID", captureID),
				zap.Reflect("new-capture", newCaptureInfo))
		}

		s.Captures[captureID] = &newCaptureInfo
		return nil
	}

	if matches := changefeedRegex.FindSubmatch(key.Bytes()); matches != nil {
		changefeedID := string(matches[1])

		if value == nil {
			log.Info("Changefeed deleted",
				zap.String("changefeedID", changefeedID),
				zap.Reflect("old-changefeed", s.ChangefeedStatuses))

			delete(s.ChangefeedStatuses, changefeedID)
			return nil
		}

		var newChangefeedStatus model.ChangeFeedStatus
		err := json.Unmarshal(value, &newChangefeedStatus)
		if err != nil {
			return errors.Trace(err)
		}

		if oldChangefeedInfo, ok := s.ChangefeedStatuses[changefeedID]; ok {
			log.Info("Changefeed updated",
				zap.String("changefeedID", changefeedID),
				zap.Reflect("old-changefeed", oldChangefeedInfo),
				zap.Reflect("new-changefeed", newChangefeedStatus))
		} else {
			log.Info("Changefeed added",
				zap.String("changefeedID", changefeedID),
				zap.Reflect("new-changefeed", newChangefeedStatus))
		}

		s.ChangefeedStatuses[changefeedID] = &newChangefeedStatus

		return nil
	}

	if matches := positionRegex.FindSubmatch(key.Bytes()); matches != nil {
		captureID := string(matches[1])
		changefeedID := string(matches[2])

		if value == nil {
			log.Info("Position deleted",
				zap.String("captureID", captureID),
				zap.String("changefeedID", changefeedID),
				zap.Reflect("old-position", s.TaskPositions[changefeedID][captureID]))

			delete(s.TaskPositions[changefeedID], captureID)
			if len(s.TaskPositions[changefeedID]) == 0 {
				delete(s.TaskPositions, changefeedID)
			}

			return nil
		}

		var newTaskPosition model.TaskPosition
		err := json.Unmarshal(value, &newTaskPosition)
		if err != nil {
			return errors.Trace(err)
		}

		if _, ok := s.TaskPositions[changefeedID]; !ok {
			s.TaskPositions[changefeedID] = make(map[model.CaptureID]*model.TaskPosition)
		}

		if position, ok := s.TaskPositions[changefeedID][captureID]; ok {
			log.Info("Position updated",
				zap.String("captureID", captureID),
				zap.String("changefeedID", changefeedID),
				zap.Reflect("old-position", position),
				zap.Reflect("new-position", newTaskPosition))
		} else {
			log.Info("Position created",
				zap.String("captureID", captureID),
				zap.String("changefeedID", changefeedID),
				zap.Reflect("new-position", newTaskPosition))
		}

		s.TaskPositions[changefeedID][captureID] = &newTaskPosition

		return nil
	}

	if matches := statusRegex.FindSubmatch(key.Bytes()); matches != nil {
		captureID := string(matches[1])
		changefeedID := string(matches[2])

		if value == nil {
			log.Info("Status deleted",
				zap.String("captureID", captureID),
				zap.String("changefeedID", changefeedID),
				zap.Reflect("old-status", s.TaskStatuses[changefeedID][captureID]))

			delete(s.TaskStatuses[changefeedID], captureID)
			if len(s.TaskStatuses[changefeedID]) == 0 {
				delete(s.TaskStatuses, changefeedID)
			}

			return nil
		}

		var newTaskStatus model.TaskStatus
		err := json.Unmarshal(value, &newTaskStatus)
		if err != nil {
			return errors.Trace(err)
		}

		if _, ok := s.TaskStatuses[changefeedID]; !ok {
			s.TaskStatuses[changefeedID] = make(map[model.CaptureID]*model.TaskStatus)
		}

		if status, ok := s.TaskStatuses[changefeedID][captureID]; ok {
			log.Info("Status updated",
				zap.String("captureID", captureID),
				zap.String("changefeedID", changefeedID),
				zap.Reflect("old-status", status),
				zap.Reflect("new-status", newTaskStatus))
		} else {
			log.Info("Status updated",
				zap.String("captureID", captureID),
				zap.String("changefeedID", changefeedID),
				zap.Reflect("new-status", newTaskStatus))
		}

		s.TaskStatuses[changefeedID][captureID] = &newTaskStatus

		return nil
	}

	log.Debug("Etcd operation ignored", zap.String("key", key.String()), zap.ByteString("value", value))
	return nil
}

func (s *cdcReactorState) GetPatches() []*orchestrator.DataPatch {
	return nil
}

*/
