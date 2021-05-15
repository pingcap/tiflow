package owner

import (
	"fmt"
	"math/rand"

	"github.com/pingcap/check"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/pkg/orchestrator"
)

var _ = check.Suite(&schedulerSuite{})

type schedulerSuite struct {
	changefeedId model.ChangeFeedID
	state        *model.ChangefeedReactorState
	tester       *orchestrator.ReactorStateTester
	captures     map[model.CaptureID]*model.CaptureInfo
	scheduler    *scheduler
}

func (s *schedulerSuite) reset(c *check.C) {
	s.changefeedId = fmt.Sprintf("test-changefeed-%x", rand.Uint32())
	s.state = model.NewChangefeedReactorState("test-changefeed")
	s.tester = orchestrator.NewReactorStateTester(c, s.state, nil)
	s.scheduler = newScheduler()
	s.captures = make(map[model.CaptureID]*model.CaptureInfo)
	s.state.PatchStatus(func(status *model.ChangeFeedStatus) (*model.ChangeFeedStatus, bool, error) {
		return &model.ChangeFeedStatus{}, true, nil
	})
	s.tester.MustApplyPatches()
}

func (s *schedulerSuite) addCapture(captureID model.CaptureID) {
	captureInfo := &model.CaptureInfo{
		ID: captureID,
	}
	s.captures[captureID] = captureInfo
	s.state.PatchTaskStatus(captureID, func(status *model.TaskStatus) (*model.TaskStatus, bool, error) {
		return &model.TaskStatus{}, true, nil
	})
	s.tester.MustApplyPatches()
}

func (s *schedulerSuite) removeCapture(captureID model.CaptureID) {
	delete(s.captures, captureID)
	// TODO
}

func (s *schedulerSuite) finishTableOperation(captureID model.CaptureID, tableIDs ...model.TableID) {
	s.state.PatchTaskStatus(captureID, func(status *model.TaskStatus) (*model.TaskStatus, bool, error) {
		for _, tableID := range tableIDs {
			status.Operation[tableID].Done = true
			status.Operation[tableID].Status = model.OperFinished
		}
		return status, true, nil
	})
	s.state.PatchTaskWorkload(captureID, func(workload model.TaskWorkload) (model.TaskWorkload, bool, error) {
		if workload == nil {
			workload = make(model.TaskWorkload)
		}
		for _, tableID := range tableIDs {
			if s.state.TaskStatuses[captureID].Operation[tableID].Delete {
				delete(workload, tableID)
			} else {
				workload[tableID] = model.WorkloadInfo{
					Workload: 1,
				}
			}
		}
		return workload, true, nil
	})
	s.tester.MustApplyPatches()
}

func (s *schedulerSuite) TestScheduleOneCapture(c *check.C) {
	s.reset(c)
	captureID := "test-capture-1"
	s.addCapture(captureID)

	// add three tables
	shouldUpdateState := s.scheduler.Tick(s.state, []model.TableID{1, 2, 3, 4}, s.captures)
	c.Assert(shouldUpdateState, check.IsFalse)
	s.tester.MustApplyPatches()
	c.Assert(s.state.TaskStatuses[captureID].Tables, check.DeepEquals, map[model.TableID]*model.TableReplicaInfo{
		1: {StartTs: 0}, 2: {StartTs: 0}, 3: {StartTs: 0}, 4: {StartTs: 0},
	})
	c.Assert(s.state.TaskStatuses[captureID].Operation, check.DeepEquals, map[model.TableID]*model.TableOperation{
		1: {Done: false, Delete: false, BoundaryTs: 0, Status: model.OperDispatched},
		2: {Done: false, Delete: false, BoundaryTs: 0, Status: model.OperDispatched},
		3: {Done: false, Delete: false, BoundaryTs: 0, Status: model.OperDispatched},
		4: {Done: false, Delete: false, BoundaryTs: 0, Status: model.OperDispatched},
	})
	shouldUpdateState = s.scheduler.Tick(s.state, []model.TableID{1, 2, 3, 4}, s.captures)
	c.Assert(shouldUpdateState, check.IsTrue)
	s.tester.MustApplyPatches()

	// two tables finish adding operation
	s.finishTableOperation(captureID, 2, 3)

	// remove table 1,2 and add table 4,5
	shouldUpdateState = s.scheduler.Tick(s.state, []model.TableID{3, 4, 5}, s.captures)
	c.Assert(shouldUpdateState, check.IsFalse)
	s.tester.MustApplyPatches()
	c.Assert(s.state.TaskStatuses[captureID].Tables, check.DeepEquals, map[model.TableID]*model.TableReplicaInfo{
		3: {StartTs: 0}, 4: {StartTs: 0}, 5: {StartTs: 0},
	})
	c.Assert(s.state.TaskStatuses[captureID].Operation, check.DeepEquals, map[model.TableID]*model.TableOperation{
		1: {Done: false, Delete: true, BoundaryTs: 0, Status: model.OperDispatched},
		2: {Done: false, Delete: true, BoundaryTs: 0, Status: model.OperDispatched},
		4: {Done: false, Delete: false, BoundaryTs: 0, Status: model.OperDispatched},
		5: {Done: false, Delete: false, BoundaryTs: 0, Status: model.OperDispatched},
	})

	// move a non exist table to a non exist capture
	s.scheduler.MoveTable(2, "fake-capture")
	// move tables to a non exist capture
	s.scheduler.MoveTable(3, "fake-capture")
	s.scheduler.MoveTable(4, "fake-capture")
	shouldUpdateState = s.scheduler.Tick(s.state, []model.TableID{3, 4, 5}, s.captures)
	c.Assert(shouldUpdateState, check.IsFalse)
	s.tester.MustApplyPatches()
	c.Assert(s.state.TaskStatuses[captureID].Tables, check.DeepEquals, map[model.TableID]*model.TableReplicaInfo{
		4: {StartTs: 0}, 5: {StartTs: 0},
	})
	c.Assert(s.state.TaskStatuses[captureID].Operation, check.DeepEquals, map[model.TableID]*model.TableOperation{
		1: {Done: false, Delete: true, BoundaryTs: 0, Status: model.OperDispatched},
		2: {Done: false, Delete: true, BoundaryTs: 0, Status: model.OperDispatched},
		3: {Done: false, Delete: true, BoundaryTs: 0, Status: model.OperDispatched},
		4: {Done: false, Delete: false, BoundaryTs: 0, Status: model.OperDispatched},
		5: {Done: false, Delete: false, BoundaryTs: 0, Status: model.OperDispatched},
	})

	// finish all operations
	s.finishTableOperation(captureID, 1, 2, 3, 4, 5)

	shouldUpdateState = s.scheduler.Tick(s.state, []model.TableID{3, 4, 5}, s.captures)
	c.Assert(shouldUpdateState, check.IsTrue)
	s.tester.MustApplyPatches()
	c.Assert(s.state.TaskStatuses[captureID].Tables, check.DeepEquals, map[model.TableID]*model.TableReplicaInfo{
		4: {StartTs: 0}, 5: {StartTs: 0},
	})
	c.Assert(s.state.TaskStatuses[captureID].Operation, check.DeepEquals, map[model.TableID]*model.TableOperation{})

	// table 3 is missing by expected, because the table was trying to move to a invalid capture
	// and the move will failed, the table 3 will be add in next tick
	shouldUpdateState = s.scheduler.Tick(s.state, []model.TableID{3, 4, 5}, s.captures)
	c.Assert(shouldUpdateState, check.IsFalse)
	s.tester.MustApplyPatches()
	c.Assert(s.state.TaskStatuses[captureID].Tables, check.DeepEquals, map[model.TableID]*model.TableReplicaInfo{
		4: {StartTs: 0}, 5: {StartTs: 0},
	})
	c.Assert(s.state.TaskStatuses[captureID].Operation, check.DeepEquals, map[model.TableID]*model.TableOperation{})

	shouldUpdateState = s.scheduler.Tick(s.state, []model.TableID{3, 4, 5}, s.captures)
	c.Assert(shouldUpdateState, check.IsFalse)
	s.tester.MustApplyPatches()
	c.Assert(s.state.TaskStatuses[captureID].Tables, check.DeepEquals, map[model.TableID]*model.TableReplicaInfo{
		3: {StartTs: 0}, 4: {StartTs: 0}, 5: {StartTs: 0},
	})
	c.Assert(s.state.TaskStatuses[captureID].Operation, check.DeepEquals, map[model.TableID]*model.TableOperation{
		3: {Done: false, Delete: false, BoundaryTs: 0, Status: model.OperDispatched},
	})
}

func (s *schedulerSuite) TestScheduleMoveTable(c *check.C) {
	s.reset(c)
	captureID1 := "test-capture-1"
	captureID2 := "test-capture-2"
	s.addCapture(captureID1)

	// add a table
	shouldUpdateState := s.scheduler.Tick(s.state, []model.TableID{1}, s.captures)
	c.Assert(shouldUpdateState, check.IsFalse)
	s.tester.MustApplyPatches()
	c.Assert(s.state.TaskStatuses[captureID1].Tables, check.DeepEquals, map[model.TableID]*model.TableReplicaInfo{
		1: {StartTs: 0},
	})
	c.Assert(s.state.TaskStatuses[captureID1].Operation, check.DeepEquals, map[model.TableID]*model.TableOperation{
		1: {Done: false, Delete: false, BoundaryTs: 0, Status: model.OperDispatched},
	})

	s.finishTableOperation(captureID1, 1)
	shouldUpdateState = s.scheduler.Tick(s.state, []model.TableID{1}, s.captures)
	c.Assert(shouldUpdateState, check.IsTrue)
	s.tester.MustApplyPatches()

	s.addCapture(captureID2)

	// add a table
	shouldUpdateState = s.scheduler.Tick(s.state, []model.TableID{1, 2}, s.captures)
	c.Assert(shouldUpdateState, check.IsFalse)
	s.tester.MustApplyPatches()
	c.Assert(s.state.TaskStatuses[captureID1].Tables, check.DeepEquals, map[model.TableID]*model.TableReplicaInfo{
		1: {StartTs: 0},
	})
	c.Assert(s.state.TaskStatuses[captureID1].Operation, check.DeepEquals, map[model.TableID]*model.TableOperation{})
	c.Assert(s.state.TaskStatuses[captureID2].Tables, check.DeepEquals, map[model.TableID]*model.TableReplicaInfo{
		2: {StartTs: 0},
	})
	c.Assert(s.state.TaskStatuses[captureID2].Operation, check.DeepEquals, map[model.TableID]*model.TableOperation{
		2: {Done: false, Delete: false, BoundaryTs: 0, Status: model.OperDispatched},
	})

	s.finishTableOperation(captureID2, 2)

	s.scheduler.MoveTable(2, captureID1)
	shouldUpdateState = s.scheduler.Tick(s.state, []model.TableID{1, 2}, s.captures)
	c.Assert(shouldUpdateState, check.IsFalse)
	s.tester.MustApplyPatches()
	c.Assert(s.state.TaskStatuses[captureID1].Tables, check.DeepEquals, map[model.TableID]*model.TableReplicaInfo{
		1: {StartTs: 0},
	})
	c.Assert(s.state.TaskStatuses[captureID1].Operation, check.DeepEquals, map[model.TableID]*model.TableOperation{})
	c.Assert(s.state.TaskStatuses[captureID2].Tables, check.DeepEquals, map[model.TableID]*model.TableReplicaInfo{})
	c.Assert(s.state.TaskStatuses[captureID2].Operation, check.DeepEquals, map[model.TableID]*model.TableOperation{
		2: {Done: false, Delete: true, BoundaryTs: 0, Status: model.OperDispatched},
	})

	s.finishTableOperation(captureID2, 2)

	shouldUpdateState = s.scheduler.Tick(s.state, []model.TableID{1, 2}, s.captures)
	c.Assert(shouldUpdateState, check.IsTrue)
	s.tester.MustApplyPatches()
	c.Assert(s.state.TaskStatuses[captureID1].Tables, check.DeepEquals, map[model.TableID]*model.TableReplicaInfo{
		1: {StartTs: 0},
	})
	c.Assert(s.state.TaskStatuses[captureID1].Operation, check.DeepEquals, map[model.TableID]*model.TableOperation{})
	c.Assert(s.state.TaskStatuses[captureID2].Tables, check.DeepEquals, map[model.TableID]*model.TableReplicaInfo{})
	c.Assert(s.state.TaskStatuses[captureID2].Operation, check.DeepEquals, map[model.TableID]*model.TableOperation{})

	shouldUpdateState = s.scheduler.Tick(s.state, []model.TableID{1, 2}, s.captures)
	c.Assert(shouldUpdateState, check.IsFalse)
	s.tester.MustApplyPatches()
	c.Assert(s.state.TaskStatuses[captureID1].Tables, check.DeepEquals, map[model.TableID]*model.TableReplicaInfo{
		1: {StartTs: 0}, 2: {StartTs: 0},
	})
	c.Assert(s.state.TaskStatuses[captureID1].Operation, check.DeepEquals, map[model.TableID]*model.TableOperation{
		2: {Done: false, Delete: false, BoundaryTs: 0, Status: model.OperDispatched},
	})
	c.Assert(s.state.TaskStatuses[captureID2].Tables, check.DeepEquals, map[model.TableID]*model.TableReplicaInfo{})
	c.Assert(s.state.TaskStatuses[captureID2].Operation, check.DeepEquals, map[model.TableID]*model.TableOperation{})
}
