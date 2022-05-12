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
	"fmt"
	"strings"

	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
)

const (
	metaPrefix = "/__cdc_meta__"

	ownerKey        = "/owner"
	captureKey      = "/capture"
	taskPositionKey = "/task/position"

	changefeedInfoKey = "/changefeed/info"
	jobKey            = "/job"

	// DeletionCounterKey is the key path for the counter of deleted keys
	DeletionCounterKey = metaPrefix + "/meta/ticdc-delete-etcd-key-count"

	// DefaultClusterAndNamespacePrefix is the default prefix of changefeed data
	DefaultClusterAndNamespacePrefix = "/tidb/cdc/default/default"
	// DefaultClusterAndMetaPrefix is the default prefix of cluster mata
	DefaultClusterAndMetaPrefix = "/tidb/cdc/default" + metaPrefix
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
	ClusterID    string
}

// BaseKey is the common prefix of the keys with cluster id in CDC
func BaseKey() string {
	clusterID := config.GetGlobalServerConfig().ClusterID
	return fmt.Sprintf("/tidb/cdc/%s", clusterID)
}

// NamespacedPrefix returns the etcd prefix of changefeed data
func NamespacedPrefix(namespace string) string {
	return BaseKey() + "/" + namespace
}

// Parse parses the given etcd key
func (k *CDCKey) Parse(key string) error {
	if !strings.HasPrefix(key, BaseKey()) {
		return cerror.ErrInvalidEtcdKey.GenWithStackByArgs(key)
	}
	key = key[len("/tidb/cdc"):]
	parts := strings.Split(key, "/")
	k.ClusterID = parts[1]
	key = key[len(k.ClusterID)+1:]
	if strings.HasPrefix(key, metaPrefix) {
		key = key[len(metaPrefix):]
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
		default:
			return cerror.ErrInvalidEtcdKey.GenWithStackByArgs(key)
		}
	} else {
		namespace := parts[2]
		key = key[len(namespace)+1:]
		switch {
		case strings.HasPrefix(key, changefeedInfoKey):
			k.Tp = CDCKeyTypeChangefeedInfo
			k.CaptureID = ""
			k.ChangefeedID = model.ChangeFeedID{
				Namespace: namespace,
				ID:        key[len(changefeedInfoKey)+1:],
			}
			k.OwnerLeaseID = ""
		case strings.HasPrefix(key, jobKey):
			k.Tp = CDCKeyTypeChangeFeedStatus
			k.CaptureID = ""
			k.ChangefeedID = model.ChangeFeedID{
				Namespace: namespace,
				ID:        key[len(jobKey)+1:],
			}
			k.OwnerLeaseID = ""
		case strings.HasPrefix(key, taskPositionKey):
			splitKey := strings.SplitN(key[len(taskPositionKey)+1:], "/", 2)
			if len(splitKey) != 2 {
				return cerror.ErrInvalidEtcdKey.GenWithStackByArgs(key)
			}
			k.Tp = CDCKeyTypeTaskPosition
			k.CaptureID = splitKey[0]
			k.ChangefeedID = model.ChangeFeedID{
				Namespace: namespace,
				ID:        splitKey[1],
			}
			k.OwnerLeaseID = ""
		default:
			return cerror.ErrInvalidEtcdKey.GenWithStackByArgs(key)
		}
	}
	return nil
}

func (k *CDCKey) String() string {
	switch k.Tp {
	case CDCKeyTypeOwner:
		if len(k.OwnerLeaseID) == 0 {
			return BaseKey() + metaPrefix + ownerKey
		}
		return BaseKey() + metaPrefix + ownerKey + "/" + k.OwnerLeaseID
	case CDCKeyTypeCapture:
		return BaseKey() + metaPrefix + captureKey + "/" + k.CaptureID
	case CDCKeyTypeChangefeedInfo:
		return NamespacedPrefix(k.ChangefeedID.Namespace) + changefeedInfoKey +
			"/" + k.ChangefeedID.ID
	case CDCKeyTypeChangeFeedStatus:
		return NamespacedPrefix(k.ChangefeedID.Namespace) + jobKey +
			"/" + k.ChangefeedID.ID
	case CDCKeyTypeTaskPosition:
		return NamespacedPrefix(k.ChangefeedID.Namespace) + taskPositionKey +
			"/" + k.CaptureID + "/" + k.ChangefeedID.ID
	}
	log.Panic("unreachable")
	return ""
}
