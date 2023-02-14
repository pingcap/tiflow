// Copyright 2022 PingCAP, Inc.
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

package model

import (
	"encoding/hex"
	"fmt"
	"path"
	"strings"

	"github.com/pingcap/log"
	pb "github.com/pingcap/tiflow/engine/enginepb"
	"github.com/pingcap/tiflow/engine/model"
	ormModel "github.com/pingcap/tiflow/engine/pkg/orm/model"
	"github.com/pingcap/tiflow/engine/pkg/tenant"
	"github.com/pingcap/tiflow/pkg/errors"
	"go.uber.org/zap"
)

type (
	// WorkerID alias worker id string
	WorkerID = string
	// JobID alias job id string
	JobID = model.JobID
	// ExecutorID alias model.ExecutorID
	ExecutorID = model.ExecutorID

	// ResourceType represents the type of the resource
	ResourceType string
	// ResourceID should be in the form of `/<type>/<unique-name>`, currently
	// only local type is available.
	ResourceID = string
	// ResourceName is a string encoding raw resource name in hexadecimal.
	// The raw resource name is the ResourceID with its type prefix removed.
	// For example, the raw resource name of `/local/resource-1` is `resource-1`.
	ResourceName = string
)

// ResourceUpdateColumns is used in gorm update
var ResourceUpdateColumns = []string{
	"updated_at",
	"project_id",
	"tenant_id",
	"id",
	"job_id",
	"worker_id",
	"executor_id",
	"deleted",
}

// ResourceKey is the unique identifier for the resource
type ResourceKey struct {
	JobID JobID
	ID    ResourceID
}

// ToResourceKeys converts resource requirements in pb to resource keys
func ToResourceKeys(requires []*pb.ResourceKey) []ResourceKey {
	if len(requires) == 0 {
		return nil
	}
	rks := make([]ResourceKey, 0, len(requires))
	for _, require := range requires {
		rks = append(rks, ResourceKey{JobID: require.GetJobId(), ID: require.GetResourceId()})
	}

	return rks
}

// ToResourceRequirement return the resource requirement of pb
func ToResourceRequirement(jobID JobID, resourceIDs ...ResourceID) []*pb.ResourceKey {
	if len(resourceIDs) == 0 {
		return nil
	}
	ress := make([]*pb.ResourceKey, 0, len(resourceIDs))
	for _, rid := range resourceIDs {
		ress = append(ress, &pb.ResourceKey{JobId: jobID, ResourceId: rid})
	}

	return ress
}

// ResourceMeta is the records stored in the metastore.
type ResourceMeta struct {
	ormModel.Model
	ProjectID tenant.ProjectID `json:"project-id" gorm:"column:project_id;type:varchar(128) not null;"`
	TenantID  tenant.Tenant    `json:"tenant-id" gorm:"column:tenant_id;type:varchar(128) not null;"`
	ID        ResourceID       `json:"id" gorm:"column:id;type:varchar(128) not null;uniqueIndex:uidx_rid,priority:2;index:idx_rei,priority:2"`
	Job       JobID            `json:"job" gorm:"column:job_id;type:varchar(128) not null;uniqueIndex:uidx_rid,priority:1"`
	Worker    WorkerID         `json:"worker" gorm:"column:worker_id;type:varchar(128) not null"`
	Executor  ExecutorID       `json:"executor" gorm:"column:executor_id;type:varchar(128) not null;index:idx_rei,priority:1"`
	GCPending bool             `json:"gc-pending" gorm:"column:gc_pending;type:BOOLEAN"`

	// TODO soft delete has not be implemented, because it requires modifying too many
	// unit tests in engine/pkg/orm
	Deleted bool `json:"deleted" gorm:"column:deleted;type:BOOLEAN"`
}

// GetID implements dataset.DataEntry
func (m *ResourceMeta) GetID() string {
	return m.ID
}

// ToQueryResourceResponse converts the ResourceMeta to pb.QueryResourceResponse
func (m *ResourceMeta) ToQueryResourceResponse() *pb.QueryResourceResponse {
	return &pb.QueryResourceResponse{
		CreatorExecutor: string(m.Executor),
		JobId:           m.Job,
		CreatorWorkerId: m.Worker,
	}
}

// Map is used in gorm update
func (m *ResourceMeta) Map() map[string]interface{} {
	return map[string]interface{}{
		"project_id":  m.ProjectID,
		"tenant_id":   m.TenantID,
		"id":          m.ID,
		"job_id":      m.Job,
		"worker_id":   m.Worker,
		"executor_id": m.Executor,
		"deleted":     m.Deleted,
	}
}

// Define all supported resource types.
const (
	ResourceTypeLocalFile = ResourceType("local")
	ResourceTypeS3        = ResourceType("s3")
	ResourceTypeGCS       = ResourceType("gs")
	ResourceTypeNone      = ResourceType("none")
)

// BuildPrefix returns the prefix of the resource type.
func (r ResourceType) BuildPrefix() string {
	// For local file, the prefix is `/local`. For S3, the prefix is `/s3`.
	return fmt.Sprintf("/%s", r)
}

// ParseResourceID returns the ResourceType and the path suffix.
func ParseResourceID(rpath ResourceID) (ResourceType, ResourceName, error) {
	if !strings.HasPrefix(rpath, "/") {
		return "", "", errors.ErrIllegalResourcePath.GenWithStackByArgs(rpath)
	}
	rpath = strings.TrimPrefix(rpath, "/")
	segments := strings.Split(rpath, "/")
	if len(segments) == 0 {
		return "", "", errors.ErrIllegalResourcePath.GenWithStackByArgs(rpath)
	}

	var resourceType ResourceType
	switch ResourceType(segments[0]) {
	case ResourceTypeLocalFile:
		resourceType = ResourceTypeLocalFile
	case ResourceTypeS3:
		resourceType = ResourceTypeS3
	case ResourceTypeGCS:
		resourceType = ResourceTypeGCS
	default:
		return "", "", errors.ErrIllegalResourcePath.GenWithStackByArgs(rpath)
	}

	suffix := path.Join(segments[1:]...)
	return resourceType, EncodeResourceName(suffix), nil
}

// BuildResourceID returns an ResourceID based on given ResourceType and ResourceName.
func BuildResourceID(rtype ResourceType, resName ResourceName) ResourceID {
	name, err := DecodeResourceName(resName)
	if err != nil {
		log.Panic("invalid resource name", zap.Error(err))
	}
	return path.Join("/"+string(rtype), name)
}

// EncodeResourceName encodes raw resource name to a valid resource name.
func EncodeResourceName(rawResName string) ResourceName {
	resName := hex.EncodeToString([]byte(rawResName))
	return resName
}

// DecodeResourceName decodes resource name to raw resource name.
func DecodeResourceName(resName ResourceName) (string, error) {
	rawResName, err := hex.DecodeString(resName)
	if err != nil {
		return "", err
	}
	return string(rawResName), nil
}
