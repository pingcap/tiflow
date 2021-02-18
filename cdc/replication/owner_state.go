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

package replication

import (
	"encoding/json"
	cerrors "github.com/pingcap/ticdc/pkg/errors"
	"regexp"
	"sort"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cdc/kv"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/pkg/orchestrator"
	"github.com/pingcap/ticdc/pkg/orchestrator/util"
	"go.uber.org/zap"
)

type ownerReactorState struct {
	Owner              model.CaptureID
	ChangeFeedInfos    map[model.ChangeFeedID]*model.ChangeFeedInfo
	Captures           map[model.CaptureID]*model.CaptureInfo
	ChangeFeedStatuses map[model.ChangeFeedID]*model.ChangeFeedStatus
	TaskPositions      map[model.ChangeFeedID]map[model.CaptureID]*model.TaskPosition
	TaskStatuses       map[model.ChangeFeedID]map[model.CaptureID]*model.TaskStatus

	patches                []*orchestrator.DataPatch
	tableToCaptureMapCache map[model.ChangeFeedID]map[model.TableID]model.CaptureID

	isInitialized bool
}

// The regex based parsing logic is only temporary.
var (
	captureRegex        = regexp.MustCompile(regexp.QuoteMeta(kv.CaptureInfoKeyPrefix) + "/(.+)")
	changeFeedRegex     = regexp.MustCompile(regexp.QuoteMeta(kv.JobKeyPrefix) + "/(.+)")
	positionRegex       = regexp.MustCompile(regexp.QuoteMeta(kv.TaskPositionKeyPrefix) + "/(.+?)/(.+)")
	statusRegex         = regexp.MustCompile(regexp.QuoteMeta(kv.TaskStatusKeyPrefix) + "/(.+?)/(.+)")
	changeFeedInfoRegex = regexp.MustCompile("/tidb/cdc/changefeed/info/(.+)")
)

func newCDCReactorState() *ownerReactorState {
	return &ownerReactorState{
		ChangeFeedInfos:        make(map[model.ChangeFeedID]*model.ChangeFeedInfo),
		Captures:               make(map[model.CaptureID]*model.CaptureInfo),
		ChangeFeedStatuses:     make(map[model.ChangeFeedID]*model.ChangeFeedStatus),
		TaskPositions:          make(map[model.ChangeFeedID]map[model.CaptureID]*model.TaskPosition),
		TaskStatuses:           make(map[model.ChangeFeedID]map[model.CaptureID]*model.TaskStatus),
		tableToCaptureMapCache: make(map[model.ChangeFeedID]map[model.TableID]model.CaptureID),
	}
}

func (s *ownerReactorState) IsInitialized() bool {
	return s.isInitialized
}

func (s *ownerReactorState) Update(key util.EtcdKey, value []byte, isInit bool) error {
	if !isInit {
		s.isInitialized = true
	}

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

	if matches := changeFeedRegex.FindSubmatch(key.Bytes()); matches != nil {
		changefeedID := string(matches[1])

		if value == nil {
			log.Info("Changefeed deleted",
				zap.String("changefeedID", changefeedID),
				zap.Reflect("old-changefeed", s.ChangeFeedStatuses))

			delete(s.ChangeFeedStatuses, changefeedID)
			return nil
		}

		var newChangefeedStatus model.ChangeFeedStatus
		err := json.Unmarshal(value, &newChangefeedStatus)
		if err != nil {
			return errors.Trace(err)
		}

		if oldChangefeedInfo, ok := s.ChangeFeedStatuses[changefeedID]; ok {
			log.Info("Changefeed updated",
				zap.String("changefeedID", changefeedID),
				zap.Reflect("old-changefeed", oldChangefeedInfo),
				zap.Reflect("new-changefeed", newChangefeedStatus))
		} else {
			log.Info("Changefeed added",
				zap.String("changefeedID", changefeedID),
				zap.Reflect("new-changefeed", newChangefeedStatus))
		}

		s.ChangeFeedStatuses[changefeedID] = &newChangefeedStatus

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

		s.tableToCaptureMapCache[changefeedID] = s.GetTableToCaptureMap(changefeedID)
		return nil
	}

	if matches := changeFeedInfoRegex.FindSubmatch(key.Bytes()); matches != nil {
		changeFeedID := string(matches[1])

		var changeFeedInfo model.ChangeFeedInfo
		err := json.Unmarshal(value, &changeFeedInfo)
		if err != nil {
			return errors.Trace(err)
		}

		s.ChangeFeedInfos[changeFeedID] = &changeFeedInfo
		return nil
	}

	log.Debug("Etcd operation ignored", zap.String("key", key.String()), zap.ByteString("value", value))
	return nil
}

func (s *ownerReactorState) GetPatches() []*orchestrator.DataPatch {
	ret := orchestrator.MergeCommutativePatches(s.patches)
	s.patches = nil
	return ret
}

func (s *ownerReactorState) UpdateChangeFeedStatus(cfID model.ChangeFeedID, resolvedTs uint64, checkpointTs uint64) {
	patch := &orchestrator.DataPatch{
		Key: util.NewEtcdKey(kv.GetEtcdKeyChangeFeedStatus(cfID)),
		Fun: func(old []byte) ([]byte, error) {
			var changeFeedStatus model.ChangeFeedStatus

			if old != nil {
				err := json.Unmarshal(old, &changeFeedStatus)
				if err != nil {
					return nil, cerrors.ErrUnmarshalFailed.Wrap(err)
				}

				if changeFeedStatus.CheckpointTs > checkpointTs {
					log.Panic("checkpointTs regressed",
						zap.Reflect("changeFeedStatus", changeFeedStatus),
						zap.Uint64("newCheckpointTs", checkpointTs))
				}
			}

			changeFeedStatus.CheckpointTs = checkpointTs
			changeFeedStatus.ResolvedTs = resolvedTs

			newBytes, err := json.Marshal(&changeFeedStatus)
			if err != nil {
				return nil, cerrors.ErrMarshalFailed.Wrap(err)
			}

			return newBytes, nil
		},
	}

	s.patches = append(s.patches, patch)
}

func (s *ownerReactorState) DispatchTable(cfID model.ChangeFeedID, captureID model.CaptureID, tableID model.TableID, replicaInfo model.TableReplicaInfo) {
	captureTaskStatuses, ok := s.TaskStatuses[cfID]
	if !ok {
		log.Panic("owner bug: changeFeedState not found", zap.String("cfID", cfID))
	}

	taskStatus, ok := captureTaskStatuses[captureID]
	if !ok {
		log.Panic("owner bug: capture not found", zap.String("captureID", captureID))
	}

	if _, ok := taskStatus.Tables[tableID]; ok {
		log.Panic("owner bug: duplicate dispatching", zap.Int64("tableID", tableID))
	}

	if _, ok := taskStatus.Operation[tableID]; ok {
		log.Panic("owner bug: duplicate dispatching", zap.Int64("tableID", tableID))
	}

	patch := &orchestrator.DataPatch{
		Key: util.NewEtcdKey(kv.GetEtcdKeyTaskStatus(cfID, captureID)),
		Fun: func(old []byte) (newValue []byte, err error) {
			var taskStatus model.TaskStatus
			if len(old) > 0 {
				err := json.Unmarshal(old, &taskStatus)
				if err != nil {
					return nil, errors.Trace(err)
				}
			}

			if _, ok := taskStatus.Operation[tableID]; ok {
				log.Panic("owner bug: duplicate dispatching", zap.Int64("tableID", tableID))
			}

			taskStatus.Tables[tableID] = &replicaInfo
			operation := &model.TableOperation{
				Delete:     false,
				BoundaryTs: replicaInfo.StartTs,
				Done:       false,
				Status:     model.OperDispatched,
			}

			taskStatus.Operation[tableID] = operation

			newValue, err = json.Marshal(&taskStatus)
			return
		},
	}

	s.patches = append(s.patches, patch)
}

func (s *ownerReactorState) RemoveTable(cfID model.ChangeFeedID, captureID model.CaptureID, tableID model.TableID) {
	captureTaskStatuses, ok := s.TaskStatuses[cfID]
	if !ok {
		log.Panic("owner bug: changeFeedState not found", zap.String("cfID", cfID))
	}

	taskStatus, ok := captureTaskStatuses[captureID]
	if !ok {
		log.Panic("owner bug: capture not found", zap.String("captureID", captureID))
	}

	if _, ok := taskStatus.Tables[tableID]; !ok {
		log.Panic("owner bug: removing table that does not exist", zap.Int64("tableID", tableID))
	}

	patch := &orchestrator.DataPatch{
		Key: util.NewEtcdKey(kv.GetEtcdKeyTaskStatus(cfID, captureID)),
		Fun: func(old []byte) ([]byte, error) {
			var taskStatus model.TaskStatus
			err := json.Unmarshal(old, &taskStatus)
			if err != nil {
				return nil, errors.Trace(err)
			}

			if _, ok := taskStatus.Tables[tableID]; !ok {
				log.Panic("owner bug: removing table that does not exist", zap.Int64("tableID", tableID))
			}

			delete(taskStatus.Tables, tableID)
			return json.Marshal(&taskStatus)
		},
	}

	s.patches = append(s.patches, patch)
}

func (s *ownerReactorState) StartDeletingTable(cfID model.ChangeFeedID, captureID model.CaptureID, tableID model.TableID) {
	captureTaskStatuses, ok := s.TaskStatuses[cfID]
	if !ok {
		log.Panic("owner bug: changeFeedState not found", zap.String("cfID", cfID))
	}

	_, ok = captureTaskStatuses[captureID]
	if !ok {
		log.Panic("owner bug: capture not found", zap.String("captureID", captureID))
	}

	patch := &orchestrator.DataPatch{
		Key: util.NewEtcdKey(kv.GetEtcdKeyTaskStatus(cfID, captureID)),
		Fun: func(old []byte) ([]byte, error) {
			var taskStatus model.TaskStatus
			err := json.Unmarshal(old, &taskStatus)
			if err != nil {
				return nil, errors.Trace(err)
			}

			taskStatus.Operation[tableID] = &model.TableOperation{
				Delete: true,
				// temporary, for testing with old processor
				BoundaryTs: s.ChangeFeedStatuses[cfID].CheckpointTs,
				Done:       false,
				Status:     model.OperDispatched,
			}
			return json.Marshal(&taskStatus)
		},
	}

	s.patches = append(s.patches, patch)
}

func (s *ownerReactorState) CleanOperation(cfID model.ChangeFeedID, captureID model.CaptureID, tableID model.TableID) {
	captureTaskStatuses, ok := s.TaskStatuses[cfID]
	if !ok {
		log.Panic("owner bug: changeFeedState not found", zap.String("cfID", cfID))
	}

	_, ok = captureTaskStatuses[captureID]
	if !ok {
		log.Panic("owner bug: capture not found", zap.String("captureID", captureID))
	}

	patch := &orchestrator.DataPatch{
		Key: util.NewEtcdKey(kv.GetEtcdKeyTaskStatus(cfID, captureID)),
		Fun: func(old []byte) ([]byte, error) {
			var taskStatus model.TaskStatus
			err := json.Unmarshal(old, &taskStatus)
			if err != nil {
				return nil, errors.Trace(err)
			}

			delete(taskStatus.Operation, tableID)
			return json.Marshal(&taskStatus)
		},
	}

	s.patches = append(s.patches, patch)
}

func (s *ownerReactorState) AlterChangeFeedRuntimeState(
	cfID model.ChangeFeedID,
	adminJobType model.AdminJobType,
	state model.FeedState,
	cfErr *model.RunningError,
	errTs int64) {

	_, ok := s.ChangeFeedInfos[cfID]
	if !ok {
		log.Panic("owner bug: changeFeedInfo not found", zap.String("cfID", cfID))
	}

	patch := &orchestrator.DataPatch{
		Key: util.NewEtcdKey(kv.GetEtcdKeyChangeFeedInfo(cfID)),
		Fun: func(old []byte) ([]byte, error) {
			if old == nil {
				log.Warn("AlterChangeFeedRuntimeState: changeFeedInfo forcibly removed", zap.String("cfID", cfID))
				return nil, cerrors.ErrEtcdIgnore
			}

			var info model.ChangeFeedInfo
			err := json.Unmarshal(old, &info)
			if err != nil {
				return nil, cerrors.ErrUnmarshalFailed.Wrap(err)
			}

			info.State = state
			info.AdminJobType = adminJobType

			if cfErr != nil {
				info.Error = cfErr
				info.ErrorHis = append(info.ErrorHis, errTs)
			}

			newBytes, err := json.Marshal(&info)
			if err != nil {
				return nil, cerrors.ErrMarshalFailed.Wrap(err)
			}

			return newBytes, nil
		},
	}

	s.patches = append(s.patches, patch)
}

func (s *ownerReactorState) CleanUpChangeFeedErrorHistory(cfID model.ChangeFeedID) {
	_, ok := s.ChangeFeedInfos[cfID]
	if !ok {
		log.Panic("owner bug: changeFeedInfo not found", zap.String("cfID", cfID))
	}

	patch := &orchestrator.DataPatch{
		Key: util.NewEtcdKey(kv.GetEtcdKeyChangeFeedInfo(cfID)),
		Fun: func(old []byte) ([]byte, error) {
			if old == nil {
				log.Warn("cleanUpChangeFeedErrorHistory: changeFeedInfo forcibly removed", zap.String("cfID", cfID))
				return nil, cerrors.ErrEtcdIgnore.GenWithStackByArgs()
			}

			var info model.ChangeFeedInfo
			err := json.Unmarshal(old, &info)
			if err != nil {
				return nil, cerrors.ErrUnmarshalFailed.Wrap(err)
			}

			i := sort.Search(len(info.ErrorHis), func(i int) bool {
				ts := info.ErrorHis[i]
				return time.Since(time.Unix(ts/1e3, (ts%1e3)*1e6)) < model.ErrorHistoryGCInterval
			})

			if i == 0 {
				return nil, cerrors.ErrEtcdIgnore.GenWithStackByArgs()
			}

			if i < len(info.ErrorHis) {
				info.ErrorHis = info.ErrorHis[i:]
			}

			newBytes, err := json.Marshal(&info)
			if err != nil {
				return nil, cerrors.ErrMarshalFailed.Wrap(err)
			}

			return newBytes, nil
		},
	}

	s.patches = append(s.patches, patch)
}

func (s *ownerReactorState) GetTableToCaptureMap(cfID model.ChangeFeedID) map[model.TableID]model.CaptureID {
	tableToCaptureMap := make(map[model.TableID]model.CaptureID)
	for captureID, taskStatus := range s.TaskStatuses[cfID] {
		for tableID := range taskStatus.Tables {
			tableToCaptureMap[tableID] = captureID
		}
	}

	return tableToCaptureMap
}

type tableProgress struct {
	resolvedTs   uint64
	checkpointTs uint64
}

func (s *ownerReactorState) GetTableProgress(cfID model.ChangeFeedID, tableID model.TableID) *tableProgress {
	m := s.tableToCaptureMapCache[cfID]
	if captureID, ok := m[tableID]; ok {
		position := s.TaskPositions[cfID][captureID]
		return &tableProgress{
			resolvedTs:   position.ResolvedTs,
			checkpointTs: position.CheckPointTs,
		}
	}

	return nil
}

func (s *ownerReactorState) GetChangeFeedActiveTables(cfID model.ChangeFeedID) []model.TableID {
	m := s.tableToCaptureMapCache[cfID]

	var tableIDs []model.TableID
	for tableID := range m {
		tableIDs = append(tableIDs, tableID)
	}

	return tableIDs
}
