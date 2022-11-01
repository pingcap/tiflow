// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.28.0
// 	protoc        v3.20.1
// source: engine/proto/resources.proto

package enginepb

import (
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	reflect "reflect"
	sync "sync"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

type CreateResourceRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	ProjectInfo     *ProjectInfo `protobuf:"bytes,1,opt,name=project_info,json=projectInfo,proto3" json:"project_info,omitempty"`
	ResourceId      string       `protobuf:"bytes,2,opt,name=resource_id,json=resourceId,proto3" json:"resource_id,omitempty"`
	CreatorExecutor string       `protobuf:"bytes,3,opt,name=creator_executor,json=creatorExecutor,proto3" json:"creator_executor,omitempty"`
	JobId           string       `protobuf:"bytes,4,opt,name=job_id,json=jobId,proto3" json:"job_id,omitempty"`
	CreatorWorkerId string       `protobuf:"bytes,5,opt,name=creator_worker_id,json=creatorWorkerId,proto3" json:"creator_worker_id,omitempty"`
}

func (x *CreateResourceRequest) Reset() {
	*x = CreateResourceRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_engine_proto_resources_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *CreateResourceRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*CreateResourceRequest) ProtoMessage() {}

func (x *CreateResourceRequest) ProtoReflect() protoreflect.Message {
	mi := &file_engine_proto_resources_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use CreateResourceRequest.ProtoReflect.Descriptor instead.
func (*CreateResourceRequest) Descriptor() ([]byte, []int) {
	return file_engine_proto_resources_proto_rawDescGZIP(), []int{0}
}

func (x *CreateResourceRequest) GetProjectInfo() *ProjectInfo {
	if x != nil {
		return x.ProjectInfo
	}
	return nil
}

func (x *CreateResourceRequest) GetResourceId() string {
	if x != nil {
		return x.ResourceId
	}
	return ""
}

func (x *CreateResourceRequest) GetCreatorExecutor() string {
	if x != nil {
		return x.CreatorExecutor
	}
	return ""
}

func (x *CreateResourceRequest) GetJobId() string {
	if x != nil {
		return x.JobId
	}
	return ""
}

func (x *CreateResourceRequest) GetCreatorWorkerId() string {
	if x != nil {
		return x.CreatorWorkerId
	}
	return ""
}

type CreateResourceResponse struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields
}

func (x *CreateResourceResponse) Reset() {
	*x = CreateResourceResponse{}
	if protoimpl.UnsafeEnabled {
		mi := &file_engine_proto_resources_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *CreateResourceResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*CreateResourceResponse) ProtoMessage() {}

func (x *CreateResourceResponse) ProtoReflect() protoreflect.Message {
	mi := &file_engine_proto_resources_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use CreateResourceResponse.ProtoReflect.Descriptor instead.
func (*CreateResourceResponse) Descriptor() ([]byte, []int) {
	return file_engine_proto_resources_proto_rawDescGZIP(), []int{1}
}

type ResourceKey struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	JobId      string `protobuf:"bytes,1,opt,name=job_id,json=jobId,proto3" json:"job_id,omitempty"`
	ResourceId string `protobuf:"bytes,2,opt,name=resource_id,json=resourceId,proto3" json:"resource_id,omitempty"`
}

func (x *ResourceKey) Reset() {
	*x = ResourceKey{}
	if protoimpl.UnsafeEnabled {
		mi := &file_engine_proto_resources_proto_msgTypes[2]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *ResourceKey) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ResourceKey) ProtoMessage() {}

func (x *ResourceKey) ProtoReflect() protoreflect.Message {
	mi := &file_engine_proto_resources_proto_msgTypes[2]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ResourceKey.ProtoReflect.Descriptor instead.
func (*ResourceKey) Descriptor() ([]byte, []int) {
	return file_engine_proto_resources_proto_rawDescGZIP(), []int{2}
}

func (x *ResourceKey) GetJobId() string {
	if x != nil {
		return x.JobId
	}
	return ""
}

func (x *ResourceKey) GetResourceId() string {
	if x != nil {
		return x.ResourceId
	}
	return ""
}

type QueryResourceRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	ResourceKey *ResourceKey `protobuf:"bytes,1,opt,name=resource_key,json=resourceKey,proto3" json:"resource_key,omitempty"`
}

func (x *QueryResourceRequest) Reset() {
	*x = QueryResourceRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_engine_proto_resources_proto_msgTypes[3]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *QueryResourceRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*QueryResourceRequest) ProtoMessage() {}

func (x *QueryResourceRequest) ProtoReflect() protoreflect.Message {
	mi := &file_engine_proto_resources_proto_msgTypes[3]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use QueryResourceRequest.ProtoReflect.Descriptor instead.
func (*QueryResourceRequest) Descriptor() ([]byte, []int) {
	return file_engine_proto_resources_proto_rawDescGZIP(), []int{3}
}

func (x *QueryResourceRequest) GetResourceKey() *ResourceKey {
	if x != nil {
		return x.ResourceKey
	}
	return nil
}

type QueryResourceResponse struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	CreatorExecutor string `protobuf:"bytes,1,opt,name=creator_executor,json=creatorExecutor,proto3" json:"creator_executor,omitempty"`
	JobId           string `protobuf:"bytes,2,opt,name=job_id,json=jobId,proto3" json:"job_id,omitempty"`
	CreatorWorkerId string `protobuf:"bytes,3,opt,name=creator_worker_id,json=creatorWorkerId,proto3" json:"creator_worker_id,omitempty"`
}

func (x *QueryResourceResponse) Reset() {
	*x = QueryResourceResponse{}
	if protoimpl.UnsafeEnabled {
		mi := &file_engine_proto_resources_proto_msgTypes[4]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *QueryResourceResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*QueryResourceResponse) ProtoMessage() {}

func (x *QueryResourceResponse) ProtoReflect() protoreflect.Message {
	mi := &file_engine_proto_resources_proto_msgTypes[4]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use QueryResourceResponse.ProtoReflect.Descriptor instead.
func (*QueryResourceResponse) Descriptor() ([]byte, []int) {
	return file_engine_proto_resources_proto_rawDescGZIP(), []int{4}
}

func (x *QueryResourceResponse) GetCreatorExecutor() string {
	if x != nil {
		return x.CreatorExecutor
	}
	return ""
}

func (x *QueryResourceResponse) GetJobId() string {
	if x != nil {
		return x.JobId
	}
	return ""
}

func (x *QueryResourceResponse) GetCreatorWorkerId() string {
	if x != nil {
		return x.CreatorWorkerId
	}
	return ""
}

type RemoveResourceRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	ResourceKey *ResourceKey `protobuf:"bytes,1,opt,name=resource_key,json=resourceKey,proto3" json:"resource_key,omitempty"`
}

func (x *RemoveResourceRequest) Reset() {
	*x = RemoveResourceRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_engine_proto_resources_proto_msgTypes[5]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *RemoveResourceRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*RemoveResourceRequest) ProtoMessage() {}

func (x *RemoveResourceRequest) ProtoReflect() protoreflect.Message {
	mi := &file_engine_proto_resources_proto_msgTypes[5]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use RemoveResourceRequest.ProtoReflect.Descriptor instead.
func (*RemoveResourceRequest) Descriptor() ([]byte, []int) {
	return file_engine_proto_resources_proto_rawDescGZIP(), []int{5}
}

func (x *RemoveResourceRequest) GetResourceKey() *ResourceKey {
	if x != nil {
		return x.ResourceKey
	}
	return nil
}

type RemoveResourceResponse struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields
}

func (x *RemoveResourceResponse) Reset() {
	*x = RemoveResourceResponse{}
	if protoimpl.UnsafeEnabled {
		mi := &file_engine_proto_resources_proto_msgTypes[6]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *RemoveResourceResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*RemoveResourceResponse) ProtoMessage() {}

func (x *RemoveResourceResponse) ProtoReflect() protoreflect.Message {
	mi := &file_engine_proto_resources_proto_msgTypes[6]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use RemoveResourceResponse.ProtoReflect.Descriptor instead.
func (*RemoveResourceResponse) Descriptor() ([]byte, []int) {
	return file_engine_proto_resources_proto_rawDescGZIP(), []int{6}
}

var File_engine_proto_resources_proto protoreflect.FileDescriptor

var file_engine_proto_resources_proto_rawDesc = []byte{
	0x0a, 0x1c, 0x65, 0x6e, 0x67, 0x69, 0x6e, 0x65, 0x2f, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x2f, 0x72,
	0x65, 0x73, 0x6f, 0x75, 0x72, 0x63, 0x65, 0x73, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12, 0x08,
	0x65, 0x6e, 0x67, 0x69, 0x6e, 0x65, 0x70, 0x62, 0x1a, 0x1b, 0x65, 0x6e, 0x67, 0x69, 0x6e, 0x65,
	0x2f, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x2f, 0x70, 0x72, 0x6f, 0x6a, 0x65, 0x63, 0x74, 0x73, 0x2e,
	0x70, 0x72, 0x6f, 0x74, 0x6f, 0x22, 0xe0, 0x01, 0x0a, 0x15, 0x43, 0x72, 0x65, 0x61, 0x74, 0x65,
	0x52, 0x65, 0x73, 0x6f, 0x75, 0x72, 0x63, 0x65, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x12,
	0x38, 0x0a, 0x0c, 0x70, 0x72, 0x6f, 0x6a, 0x65, 0x63, 0x74, 0x5f, 0x69, 0x6e, 0x66, 0x6f, 0x18,
	0x01, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x15, 0x2e, 0x65, 0x6e, 0x67, 0x69, 0x6e, 0x65, 0x70, 0x62,
	0x2e, 0x50, 0x72, 0x6f, 0x6a, 0x65, 0x63, 0x74, 0x49, 0x6e, 0x66, 0x6f, 0x52, 0x0b, 0x70, 0x72,
	0x6f, 0x6a, 0x65, 0x63, 0x74, 0x49, 0x6e, 0x66, 0x6f, 0x12, 0x1f, 0x0a, 0x0b, 0x72, 0x65, 0x73,
	0x6f, 0x75, 0x72, 0x63, 0x65, 0x5f, 0x69, 0x64, 0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x0a,
	0x72, 0x65, 0x73, 0x6f, 0x75, 0x72, 0x63, 0x65, 0x49, 0x64, 0x12, 0x29, 0x0a, 0x10, 0x63, 0x72,
	0x65, 0x61, 0x74, 0x6f, 0x72, 0x5f, 0x65, 0x78, 0x65, 0x63, 0x75, 0x74, 0x6f, 0x72, 0x18, 0x03,
	0x20, 0x01, 0x28, 0x09, 0x52, 0x0f, 0x63, 0x72, 0x65, 0x61, 0x74, 0x6f, 0x72, 0x45, 0x78, 0x65,
	0x63, 0x75, 0x74, 0x6f, 0x72, 0x12, 0x15, 0x0a, 0x06, 0x6a, 0x6f, 0x62, 0x5f, 0x69, 0x64, 0x18,
	0x04, 0x20, 0x01, 0x28, 0x09, 0x52, 0x05, 0x6a, 0x6f, 0x62, 0x49, 0x64, 0x12, 0x2a, 0x0a, 0x11,
	0x63, 0x72, 0x65, 0x61, 0x74, 0x6f, 0x72, 0x5f, 0x77, 0x6f, 0x72, 0x6b, 0x65, 0x72, 0x5f, 0x69,
	0x64, 0x18, 0x05, 0x20, 0x01, 0x28, 0x09, 0x52, 0x0f, 0x63, 0x72, 0x65, 0x61, 0x74, 0x6f, 0x72,
	0x57, 0x6f, 0x72, 0x6b, 0x65, 0x72, 0x49, 0x64, 0x22, 0x18, 0x0a, 0x16, 0x43, 0x72, 0x65, 0x61,
	0x74, 0x65, 0x52, 0x65, 0x73, 0x6f, 0x75, 0x72, 0x63, 0x65, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e,
	0x73, 0x65, 0x22, 0x45, 0x0a, 0x0b, 0x52, 0x65, 0x73, 0x6f, 0x75, 0x72, 0x63, 0x65, 0x4b, 0x65,
	0x79, 0x12, 0x15, 0x0a, 0x06, 0x6a, 0x6f, 0x62, 0x5f, 0x69, 0x64, 0x18, 0x01, 0x20, 0x01, 0x28,
	0x09, 0x52, 0x05, 0x6a, 0x6f, 0x62, 0x49, 0x64, 0x12, 0x1f, 0x0a, 0x0b, 0x72, 0x65, 0x73, 0x6f,
	0x75, 0x72, 0x63, 0x65, 0x5f, 0x69, 0x64, 0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x0a, 0x72,
	0x65, 0x73, 0x6f, 0x75, 0x72, 0x63, 0x65, 0x49, 0x64, 0x22, 0x50, 0x0a, 0x14, 0x51, 0x75, 0x65,
	0x72, 0x79, 0x52, 0x65, 0x73, 0x6f, 0x75, 0x72, 0x63, 0x65, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73,
	0x74, 0x12, 0x38, 0x0a, 0x0c, 0x72, 0x65, 0x73, 0x6f, 0x75, 0x72, 0x63, 0x65, 0x5f, 0x6b, 0x65,
	0x79, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x15, 0x2e, 0x65, 0x6e, 0x67, 0x69, 0x6e, 0x65,
	0x70, 0x62, 0x2e, 0x52, 0x65, 0x73, 0x6f, 0x75, 0x72, 0x63, 0x65, 0x4b, 0x65, 0x79, 0x52, 0x0b,
	0x72, 0x65, 0x73, 0x6f, 0x75, 0x72, 0x63, 0x65, 0x4b, 0x65, 0x79, 0x22, 0x85, 0x01, 0x0a, 0x15,
	0x51, 0x75, 0x65, 0x72, 0x79, 0x52, 0x65, 0x73, 0x6f, 0x75, 0x72, 0x63, 0x65, 0x52, 0x65, 0x73,
	0x70, 0x6f, 0x6e, 0x73, 0x65, 0x12, 0x29, 0x0a, 0x10, 0x63, 0x72, 0x65, 0x61, 0x74, 0x6f, 0x72,
	0x5f, 0x65, 0x78, 0x65, 0x63, 0x75, 0x74, 0x6f, 0x72, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52,
	0x0f, 0x63, 0x72, 0x65, 0x61, 0x74, 0x6f, 0x72, 0x45, 0x78, 0x65, 0x63, 0x75, 0x74, 0x6f, 0x72,
	0x12, 0x15, 0x0a, 0x06, 0x6a, 0x6f, 0x62, 0x5f, 0x69, 0x64, 0x18, 0x02, 0x20, 0x01, 0x28, 0x09,
	0x52, 0x05, 0x6a, 0x6f, 0x62, 0x49, 0x64, 0x12, 0x2a, 0x0a, 0x11, 0x63, 0x72, 0x65, 0x61, 0x74,
	0x6f, 0x72, 0x5f, 0x77, 0x6f, 0x72, 0x6b, 0x65, 0x72, 0x5f, 0x69, 0x64, 0x18, 0x03, 0x20, 0x01,
	0x28, 0x09, 0x52, 0x0f, 0x63, 0x72, 0x65, 0x61, 0x74, 0x6f, 0x72, 0x57, 0x6f, 0x72, 0x6b, 0x65,
	0x72, 0x49, 0x64, 0x22, 0x51, 0x0a, 0x15, 0x52, 0x65, 0x6d, 0x6f, 0x76, 0x65, 0x52, 0x65, 0x73,
	0x6f, 0x75, 0x72, 0x63, 0x65, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x12, 0x38, 0x0a, 0x0c,
	0x72, 0x65, 0x73, 0x6f, 0x75, 0x72, 0x63, 0x65, 0x5f, 0x6b, 0x65, 0x79, 0x18, 0x01, 0x20, 0x01,
	0x28, 0x0b, 0x32, 0x15, 0x2e, 0x65, 0x6e, 0x67, 0x69, 0x6e, 0x65, 0x70, 0x62, 0x2e, 0x52, 0x65,
	0x73, 0x6f, 0x75, 0x72, 0x63, 0x65, 0x4b, 0x65, 0x79, 0x52, 0x0b, 0x72, 0x65, 0x73, 0x6f, 0x75,
	0x72, 0x63, 0x65, 0x4b, 0x65, 0x79, 0x22, 0x18, 0x0a, 0x16, 0x52, 0x65, 0x6d, 0x6f, 0x76, 0x65,
	0x52, 0x65, 0x73, 0x6f, 0x75, 0x72, 0x63, 0x65, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65,
	0x32, 0x93, 0x02, 0x0a, 0x0f, 0x52, 0x65, 0x73, 0x6f, 0x75, 0x72, 0x63, 0x65, 0x4d, 0x61, 0x6e,
	0x61, 0x67, 0x65, 0x72, 0x12, 0x55, 0x0a, 0x0e, 0x43, 0x72, 0x65, 0x61, 0x74, 0x65, 0x52, 0x65,
	0x73, 0x6f, 0x75, 0x72, 0x63, 0x65, 0x12, 0x1f, 0x2e, 0x65, 0x6e, 0x67, 0x69, 0x6e, 0x65, 0x70,
	0x62, 0x2e, 0x43, 0x72, 0x65, 0x61, 0x74, 0x65, 0x52, 0x65, 0x73, 0x6f, 0x75, 0x72, 0x63, 0x65,
	0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x1a, 0x20, 0x2e, 0x65, 0x6e, 0x67, 0x69, 0x6e, 0x65,
	0x70, 0x62, 0x2e, 0x43, 0x72, 0x65, 0x61, 0x74, 0x65, 0x52, 0x65, 0x73, 0x6f, 0x75, 0x72, 0x63,
	0x65, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x22, 0x00, 0x12, 0x52, 0x0a, 0x0d, 0x51,
	0x75, 0x65, 0x72, 0x79, 0x52, 0x65, 0x73, 0x6f, 0x75, 0x72, 0x63, 0x65, 0x12, 0x1e, 0x2e, 0x65,
	0x6e, 0x67, 0x69, 0x6e, 0x65, 0x70, 0x62, 0x2e, 0x51, 0x75, 0x65, 0x72, 0x79, 0x52, 0x65, 0x73,
	0x6f, 0x75, 0x72, 0x63, 0x65, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x1a, 0x1f, 0x2e, 0x65,
	0x6e, 0x67, 0x69, 0x6e, 0x65, 0x70, 0x62, 0x2e, 0x51, 0x75, 0x65, 0x72, 0x79, 0x52, 0x65, 0x73,
	0x6f, 0x75, 0x72, 0x63, 0x65, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x22, 0x00, 0x12,
	0x55, 0x0a, 0x0e, 0x52, 0x65, 0x6d, 0x6f, 0x76, 0x65, 0x52, 0x65, 0x73, 0x6f, 0x75, 0x72, 0x63,
	0x65, 0x12, 0x1f, 0x2e, 0x65, 0x6e, 0x67, 0x69, 0x6e, 0x65, 0x70, 0x62, 0x2e, 0x52, 0x65, 0x6d,
	0x6f, 0x76, 0x65, 0x52, 0x65, 0x73, 0x6f, 0x75, 0x72, 0x63, 0x65, 0x52, 0x65, 0x71, 0x75, 0x65,
	0x73, 0x74, 0x1a, 0x20, 0x2e, 0x65, 0x6e, 0x67, 0x69, 0x6e, 0x65, 0x70, 0x62, 0x2e, 0x52, 0x65,
	0x6d, 0x6f, 0x76, 0x65, 0x52, 0x65, 0x73, 0x6f, 0x75, 0x72, 0x63, 0x65, 0x52, 0x65, 0x73, 0x70,
	0x6f, 0x6e, 0x73, 0x65, 0x22, 0x00, 0x42, 0x2b, 0x5a, 0x29, 0x67, 0x69, 0x74, 0x68, 0x75, 0x62,
	0x2e, 0x63, 0x6f, 0x6d, 0x2f, 0x70, 0x69, 0x6e, 0x67, 0x63, 0x61, 0x70, 0x2f, 0x74, 0x69, 0x66,
	0x6c, 0x6f, 0x77, 0x2f, 0x65, 0x6e, 0x67, 0x69, 0x6e, 0x65, 0x2f, 0x65, 0x6e, 0x67, 0x69, 0x6e,
	0x65, 0x70, 0x62, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_engine_proto_resources_proto_rawDescOnce sync.Once
	file_engine_proto_resources_proto_rawDescData = file_engine_proto_resources_proto_rawDesc
)

func file_engine_proto_resources_proto_rawDescGZIP() []byte {
	file_engine_proto_resources_proto_rawDescOnce.Do(func() {
		file_engine_proto_resources_proto_rawDescData = protoimpl.X.CompressGZIP(file_engine_proto_resources_proto_rawDescData)
	})
	return file_engine_proto_resources_proto_rawDescData
}

var file_engine_proto_resources_proto_msgTypes = make([]protoimpl.MessageInfo, 7)
var file_engine_proto_resources_proto_goTypes = []interface{}{
	(*CreateResourceRequest)(nil),  // 0: enginepb.CreateResourceRequest
	(*CreateResourceResponse)(nil), // 1: enginepb.CreateResourceResponse
	(*ResourceKey)(nil),            // 2: enginepb.ResourceKey
	(*QueryResourceRequest)(nil),   // 3: enginepb.QueryResourceRequest
	(*QueryResourceResponse)(nil),  // 4: enginepb.QueryResourceResponse
	(*RemoveResourceRequest)(nil),  // 5: enginepb.RemoveResourceRequest
	(*RemoveResourceResponse)(nil), // 6: enginepb.RemoveResourceResponse
	(*ProjectInfo)(nil),            // 7: enginepb.ProjectInfo
}
var file_engine_proto_resources_proto_depIdxs = []int32{
	7, // 0: enginepb.CreateResourceRequest.project_info:type_name -> enginepb.ProjectInfo
	2, // 1: enginepb.QueryResourceRequest.resource_key:type_name -> enginepb.ResourceKey
	2, // 2: enginepb.RemoveResourceRequest.resource_key:type_name -> enginepb.ResourceKey
	0, // 3: enginepb.ResourceManager.CreateResource:input_type -> enginepb.CreateResourceRequest
	3, // 4: enginepb.ResourceManager.QueryResource:input_type -> enginepb.QueryResourceRequest
	5, // 5: enginepb.ResourceManager.RemoveResource:input_type -> enginepb.RemoveResourceRequest
	1, // 6: enginepb.ResourceManager.CreateResource:output_type -> enginepb.CreateResourceResponse
	4, // 7: enginepb.ResourceManager.QueryResource:output_type -> enginepb.QueryResourceResponse
	6, // 8: enginepb.ResourceManager.RemoveResource:output_type -> enginepb.RemoveResourceResponse
	6, // [6:9] is the sub-list for method output_type
	3, // [3:6] is the sub-list for method input_type
	3, // [3:3] is the sub-list for extension type_name
	3, // [3:3] is the sub-list for extension extendee
	0, // [0:3] is the sub-list for field type_name
}

func init() { file_engine_proto_resources_proto_init() }
func file_engine_proto_resources_proto_init() {
	if File_engine_proto_resources_proto != nil {
		return
	}
	file_engine_proto_projects_proto_init()
	if !protoimpl.UnsafeEnabled {
		file_engine_proto_resources_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*CreateResourceRequest); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_engine_proto_resources_proto_msgTypes[1].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*CreateResourceResponse); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_engine_proto_resources_proto_msgTypes[2].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*ResourceKey); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_engine_proto_resources_proto_msgTypes[3].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*QueryResourceRequest); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_engine_proto_resources_proto_msgTypes[4].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*QueryResourceResponse); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_engine_proto_resources_proto_msgTypes[5].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*RemoveResourceRequest); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_engine_proto_resources_proto_msgTypes[6].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*RemoveResourceResponse); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_engine_proto_resources_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   7,
			NumExtensions: 0,
			NumServices:   1,
		},
		GoTypes:           file_engine_proto_resources_proto_goTypes,
		DependencyIndexes: file_engine_proto_resources_proto_depIdxs,
		MessageInfos:      file_engine_proto_resources_proto_msgTypes,
	}.Build()
	File_engine_proto_resources_proto = out.File
	file_engine_proto_resources_proto_rawDesc = nil
	file_engine_proto_resources_proto_goTypes = nil
	file_engine_proto_resources_proto_depIdxs = nil
}
