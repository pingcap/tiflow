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

import (
	"log"
	"strings"

	"github.com/pingcap/ticdc/cdc/model"
	cerror "github.com/pingcap/ticdc/pkg/errors"
)

const (
	etcdKeyBase = "/tidb/cdc"
	ownerKey    = "/owner"
	captureKey  = "/capture"

	taskKey         = "/task"
	taskWorkloadKey = taskKey + "/workload"
	taskStatusKey   = taskKey + "/status"
	taskPositionKey = taskKey + "/position"

	changefeedInfoKey = "/changefeed/info"
	jobKey            = "/job"
)

// CDCEtcdKeyType is the type of etcd key
type CDCEtcdKeyType = int

// the types of etcd key
const (
	CDCEtcdKeyTypeUnknown CDCEtcdKeyType = iota
	CDCEtcdKeyTypeOnwer
	CDCEtcdKeyTypeCapture
	CDCEtcdKeyTypeChangefeedInfo
	CDCEtcdKeyTypeChangeFeedStatus
	CDCEtcdKeyTypeTaskPosition
	CDCEtcdKeyTypeTaskStatus
	CDCEtcdKeyTypeTaskWorkload
)

// CDCEtcdKey represents a etcd key which is defined by TiCDC
type CDCEtcdKey struct {
	Tp           CDCEtcdKeyType
	ChangefeedID model.ChangeFeedID
	CaptureID    model.CaptureID
	OwnerLeaseID string
}

// Parse parses the given etcd key
func (k *CDCEtcdKey) Parse(key string) error {
	if !strings.HasPrefix(key, etcdKeyBase) {
		return cerror.ErrInvalidEtcdKey.GenWithStackByArgs(key)
	}
	key = key[len(etcdKeyBase):]
	switch {
	case strings.HasPrefix(key, ownerKey):
		k.Tp = CDCEtcdKeyTypeOnwer
		k.CaptureID = ""
		k.ChangefeedID = ""
		key = key[len(ownerKey):]
		if len(key) > 0 {
			key = key[1:]
		}
		k.OwnerLeaseID = key
	case strings.HasPrefix(key, captureKey):
		k.Tp = CDCEtcdKeyTypeCapture
		k.CaptureID = key[len(captureKey)+1:]
		k.ChangefeedID = ""
		k.OwnerLeaseID = ""
	case strings.HasPrefix(key, changefeedInfoKey):
		k.Tp = CDCEtcdKeyTypeChangefeedInfo
		k.CaptureID = ""
		k.ChangefeedID = key[len(changefeedInfoKey)+1:]
		k.OwnerLeaseID = ""
	case strings.HasPrefix(key, jobKey):
		k.Tp = CDCEtcdKeyTypeChangeFeedStatus
		k.CaptureID = ""
		k.ChangefeedID = key[len(jobKey)+1:]
		k.OwnerLeaseID = ""
	case strings.HasPrefix(key, taskStatusKey):
		splitKey := strings.SplitN(key[len(taskStatusKey)+1:], "/", 2)
		if len(splitKey) != 2 {
			return cerror.ErrInvalidEtcdKey.GenWithStackByArgs(key)
		}
		k.Tp = CDCEtcdKeyTypeTaskStatus
		k.CaptureID = splitKey[0]
		k.ChangefeedID = splitKey[1]
		k.OwnerLeaseID = ""
	case strings.HasPrefix(key, taskPositionKey):
		splitKey := strings.SplitN(key[len(taskPositionKey)+1:], "/", 2)
		if len(splitKey) != 2 {
			return cerror.ErrInvalidEtcdKey.GenWithStackByArgs(key)
		}
		k.Tp = CDCEtcdKeyTypeTaskPosition
		k.CaptureID = splitKey[0]
		k.ChangefeedID = splitKey[1]
		k.OwnerLeaseID = ""
	case strings.HasPrefix(key, taskWorkloadKey):
		splitKey := strings.SplitN(key[len(taskWorkloadKey)+1:], "/", 2)
		if len(splitKey) != 2 {
			return cerror.ErrInvalidEtcdKey.GenWithStackByArgs(key)
		}
		k.Tp = CDCEtcdKeyTypeTaskWorkload
		k.CaptureID = splitKey[0]
		k.ChangefeedID = splitKey[1]
		k.OwnerLeaseID = ""
	default:
		return cerror.ErrInvalidEtcdKey.GenWithStackByArgs(key)
	}
	return nil
}

func (k *CDCEtcdKey) String() string {
	switch k.Tp {
	case CDCEtcdKeyTypeOnwer:
		if len(k.OwnerLeaseID) == 0 {
			return etcdKeyBase + ownerKey
		}
		return etcdKeyBase + ownerKey + "/" + k.OwnerLeaseID
	case CDCEtcdKeyTypeCapture:
		return etcdKeyBase + captureKey + "/" + k.CaptureID
	case CDCEtcdKeyTypeChangefeedInfo:
		return etcdKeyBase + changefeedInfoKey + "/" + k.ChangefeedID
	case CDCEtcdKeyTypeChangeFeedStatus:
		return etcdKeyBase + jobKey + "/" + k.ChangefeedID
	case CDCEtcdKeyTypeTaskPosition:
		return etcdKeyBase + taskPositionKey + "/" + k.CaptureID + "/" + k.ChangefeedID
	case CDCEtcdKeyTypeTaskStatus:
		return etcdKeyBase + taskStatusKey + "/" + k.CaptureID + "/" + k.ChangefeedID
	case CDCEtcdKeyTypeTaskWorkload:
		return etcdKeyBase + taskWorkloadKey + "/" + k.CaptureID + "/" + k.ChangefeedID
	default:
		log.Panic("unreachable")
	}
	return ""
}
