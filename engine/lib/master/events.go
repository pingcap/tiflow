package master

import libModel "github.com/hanfei1991/microcosm/lib/model"

type masterEventType int32

const (
	workerOnlineEvent = masterEventType(iota + 1)
	workerOfflineEvent
	workerStatusUpdatedEvent
	workerDispatchFailedEvent
)

type beforeHookType = func() (ok bool)

type masterEvent struct {
	Tp         masterEventType
	Handle     WorkerHandle
	WorkerID   libModel.WorkerID
	Err        error
	beforeHook beforeHookType
}
