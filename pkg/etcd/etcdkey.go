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

package etcd

import (
	"strings"

	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	cerror "github.com/pingcap/tiflow/pkg/errors"
)

const (
	// EtcdKeyBase is the common prefix of the keys in CDC
	EtcdKeyBase = "/tidb/cdc"
	ownerKey    = "/owner"
	captureKey  = "/capture"

	taskKey         = "/task"
	taskWorkloadKey = taskKey + "/workload"
	taskStatusKey   = taskKey + "/status"
	taskPositionKey = taskKey + "/position"

	changefeedInfoKey = "/changefeed/info"
	jobKey            = "/job"
)

// CDCKeyType is the type of etcd key
type CDCKeyType = int

// the types of etcd key
const (
	CDCKeyTypeUnknown CDCKeyType = iota
	CDCKeyTypeOwner
	CDCKeyTypeCapture
	CDCKeyTypeChangefeedInfo
	CDCKeyTypeChangeFeedStatus
	CDCKeyTypeTaskPosition
	// Deprecated: No longer used. Kept for compatibility.
	CDCKeyTypeTaskStatus
	// Deprecated: No longer used. Kept for compatibility.
	CDCKeyTypeTaskWorkload
)

// CDCKey represents a etcd key which is defined by TiCDC
/*
 Usage:
 we can parse a raw etcd key:
 ```
 	k := new(CDCKey)
 	rawKey := "/tidb/cdc/changefeed/info/test/changefeed"
	err := k.Parse(rawKey)
 	c.Assert(k, check.DeepEquals, &CDCKey{
			Tp:           CDCKeyTypeChangefeedInfo,
			ChangefeedID: "test/changefeed",
	})
 ```

 and we can generate a raw key from CDCKey
 ```
 	k := &CDCKey{
			Tp:           CDCKeyTypeChangefeedInfo,
			ChangefeedID: "test/changefeed",
	}
 	c.Assert(k.String(), check.Equals, "/tidb/cdc/changefeed/info/test/changefeed")
 ```

*/
type CDCKey struct {
	Tp           CDCKeyType
	ChangefeedID model.ChangeFeedID
	CaptureID    string
	OwnerLeaseID string
}

// Parse parses the given etcd key
func (k *CDCKey) Parse(key string) error {
	if !strings.HasPrefix(key, EtcdKeyBase) {
		return cerror.ErrInvalidEtcdKey.GenWithStackByArgs(key)
	}
	key = key[len(EtcdKeyBase):]
	switch {
	case strings.HasPrefix(key, ownerKey):
		k.Tp = CDCKeyTypeOwner
		k.CaptureID = ""
		key = key[len(ownerKey):]
		if len(key) > 0 {
			key = key[1:]
		}
		k.OwnerLeaseID = key
	case strings.HasPrefix(key, captureKey):
		k.Tp = CDCKeyTypeCapture
		k.CaptureID = key[len(captureKey)+1:]
		k.OwnerLeaseID = ""
	case strings.HasPrefix(key, changefeedInfoKey):
		k.Tp = CDCKeyTypeChangefeedInfo
		k.CaptureID = ""
		k.ChangefeedID = model.DefaultChangeFeedID(key[len(changefeedInfoKey)+1:])
		k.OwnerLeaseID = ""
	case strings.HasPrefix(key, jobKey):
		k.Tp = CDCKeyTypeChangeFeedStatus
		k.CaptureID = ""
		k.ChangefeedID = model.DefaultChangeFeedID(key[len(jobKey)+1:])
		k.OwnerLeaseID = ""
	case strings.HasPrefix(key, taskStatusKey):
		splitKey := strings.SplitN(key[len(taskStatusKey)+1:], "/", 2)
		if len(splitKey) != 2 {
			return cerror.ErrInvalidEtcdKey.GenWithStackByArgs(key)
		}
		k.Tp = CDCKeyTypeTaskStatus
		k.CaptureID = splitKey[0]
		k.ChangefeedID = model.DefaultChangeFeedID(splitKey[1])
		k.OwnerLeaseID = ""
	case strings.HasPrefix(key, taskPositionKey):
		splitKey := strings.SplitN(key[len(taskPositionKey)+1:], "/", 2)
		if len(splitKey) != 2 {
			return cerror.ErrInvalidEtcdKey.GenWithStackByArgs(key)
		}
		k.Tp = CDCKeyTypeTaskPosition
		k.CaptureID = splitKey[0]
		k.ChangefeedID = model.DefaultChangeFeedID(splitKey[1])
		k.OwnerLeaseID = ""
	case strings.HasPrefix(key, taskWorkloadKey):
		splitKey := strings.SplitN(key[len(taskWorkloadKey)+1:], "/", 2)
		if len(splitKey) != 2 {
			return cerror.ErrInvalidEtcdKey.GenWithStackByArgs(key)
		}
		k.Tp = CDCKeyTypeTaskWorkload
		k.CaptureID = splitKey[0]
		k.ChangefeedID = model.DefaultChangeFeedID(splitKey[1])
		k.OwnerLeaseID = ""
	default:
		return cerror.ErrInvalidEtcdKey.GenWithStackByArgs(key)
	}
	return nil
}

func (k *CDCKey) String() string {
	switch k.Tp {
	case CDCKeyTypeOwner:
		if len(k.OwnerLeaseID) == 0 {
			return EtcdKeyBase + ownerKey
		}
		return EtcdKeyBase + ownerKey + "/" + k.OwnerLeaseID
	case CDCKeyTypeCapture:
		return EtcdKeyBase + captureKey + "/" + k.CaptureID
	case CDCKeyTypeChangefeedInfo:
		return EtcdKeyBase + changefeedInfoKey + "/" + k.ChangefeedID.ID
	case CDCKeyTypeChangeFeedStatus:
		return EtcdKeyBase + jobKey + "/" + k.ChangefeedID.ID
	case CDCKeyTypeTaskPosition:
		return EtcdKeyBase + taskPositionKey + "/" + k.CaptureID + "/" + k.ChangefeedID.ID
	case CDCKeyTypeTaskStatus:
		return EtcdKeyBase + taskStatusKey + "/" + k.CaptureID + "/" + k.ChangefeedID.ID
	case CDCKeyTypeTaskWorkload:
		return EtcdKeyBase + taskWorkloadKey + "/" + k.CaptureID + "/" + k.ChangefeedID.ID
	}
	log.Panic("unreachable")
	return ""
}
