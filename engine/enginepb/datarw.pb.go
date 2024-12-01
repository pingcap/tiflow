// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.34.2
// 	protoc        v3.20.1
// source: engine/proto/datarw.proto

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

type GenerateDataRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	FileNum   int32 `protobuf:"varint,1,opt,name=file_num,json=fileNum,proto3" json:"file_num,omitempty"`
	RecordNum int32 `protobuf:"varint,2,opt,name=record_num,json=recordNum,proto3" json:"record_num,omitempty"`
}

func (x *GenerateDataRequest) Reset() {
	*x = GenerateDataRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_engine_proto_datarw_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *GenerateDataRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*GenerateDataRequest) ProtoMessage() {}

func (x *GenerateDataRequest) ProtoReflect() protoreflect.Message {
	mi := &file_engine_proto_datarw_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use GenerateDataRequest.ProtoReflect.Descriptor instead.
func (*GenerateDataRequest) Descriptor() ([]byte, []int) {
	return file_engine_proto_datarw_proto_rawDescGZIP(), []int{0}
}

func (x *GenerateDataRequest) GetFileNum() int32 {
	if x != nil {
		return x.FileNum
	}
	return 0
}

func (x *GenerateDataRequest) GetRecordNum() int32 {
	if x != nil {
		return x.RecordNum
	}
	return 0
}

type GenerateDataResponse struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	ErrMsg string `protobuf:"bytes,1,opt,name=err_msg,json=errMsg,proto3" json:"err_msg,omitempty"`
}

func (x *GenerateDataResponse) Reset() {
	*x = GenerateDataResponse{}
	if protoimpl.UnsafeEnabled {
		mi := &file_engine_proto_datarw_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *GenerateDataResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*GenerateDataResponse) ProtoMessage() {}

func (x *GenerateDataResponse) ProtoReflect() protoreflect.Message {
	mi := &file_engine_proto_datarw_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use GenerateDataResponse.ProtoReflect.Descriptor instead.
func (*GenerateDataResponse) Descriptor() ([]byte, []int) {
	return file_engine_proto_datarw_proto_rawDescGZIP(), []int{1}
}

func (x *GenerateDataResponse) GetErrMsg() string {
	if x != nil {
		return x.ErrMsg
	}
	return ""
}

type CheckDirRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Dir string `protobuf:"bytes,1,opt,name=dir,proto3" json:"dir,omitempty"`
}

func (x *CheckDirRequest) Reset() {
	*x = CheckDirRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_engine_proto_datarw_proto_msgTypes[2]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *CheckDirRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*CheckDirRequest) ProtoMessage() {}

func (x *CheckDirRequest) ProtoReflect() protoreflect.Message {
	mi := &file_engine_proto_datarw_proto_msgTypes[2]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use CheckDirRequest.ProtoReflect.Descriptor instead.
func (*CheckDirRequest) Descriptor() ([]byte, []int) {
	return file_engine_proto_datarw_proto_rawDescGZIP(), []int{2}
}

func (x *CheckDirRequest) GetDir() string {
	if x != nil {
		return x.Dir
	}
	return ""
}

type CheckDirResponse struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	ErrMsg     string `protobuf:"bytes,1,opt,name=err_msg,json=errMsg,proto3" json:"err_msg,omitempty"`
	ErrFileIdx int32  `protobuf:"varint,2,opt,name=err_file_idx,json=errFileIdx,proto3" json:"err_file_idx,omitempty"`
}

func (x *CheckDirResponse) Reset() {
	*x = CheckDirResponse{}
	if protoimpl.UnsafeEnabled {
		mi := &file_engine_proto_datarw_proto_msgTypes[3]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *CheckDirResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*CheckDirResponse) ProtoMessage() {}

func (x *CheckDirResponse) ProtoReflect() protoreflect.Message {
	mi := &file_engine_proto_datarw_proto_msgTypes[3]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use CheckDirResponse.ProtoReflect.Descriptor instead.
func (*CheckDirResponse) Descriptor() ([]byte, []int) {
	return file_engine_proto_datarw_proto_rawDescGZIP(), []int{3}
}

func (x *CheckDirResponse) GetErrMsg() string {
	if x != nil {
		return x.ErrMsg
	}
	return ""
}

func (x *CheckDirResponse) GetErrFileIdx() int32 {
	if x != nil {
		return x.ErrFileIdx
	}
	return 0
}

type IsReadyRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields
}

func (x *IsReadyRequest) Reset() {
	*x = IsReadyRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_engine_proto_datarw_proto_msgTypes[4]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *IsReadyRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*IsReadyRequest) ProtoMessage() {}

func (x *IsReadyRequest) ProtoReflect() protoreflect.Message {
	mi := &file_engine_proto_datarw_proto_msgTypes[4]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use IsReadyRequest.ProtoReflect.Descriptor instead.
func (*IsReadyRequest) Descriptor() ([]byte, []int) {
	return file_engine_proto_datarw_proto_rawDescGZIP(), []int{4}
}

type IsReadyResponse struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Ready bool `protobuf:"varint,1,opt,name=ready,proto3" json:"ready,omitempty"`
}

func (x *IsReadyResponse) Reset() {
	*x = IsReadyResponse{}
	if protoimpl.UnsafeEnabled {
		mi := &file_engine_proto_datarw_proto_msgTypes[5]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *IsReadyResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*IsReadyResponse) ProtoMessage() {}

func (x *IsReadyResponse) ProtoReflect() protoreflect.Message {
	mi := &file_engine_proto_datarw_proto_msgTypes[5]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use IsReadyResponse.ProtoReflect.Descriptor instead.
func (*IsReadyResponse) Descriptor() ([]byte, []int) {
	return file_engine_proto_datarw_proto_rawDescGZIP(), []int{5}
}

func (x *IsReadyResponse) GetReady() bool {
	if x != nil {
		return x.Ready
	}
	return false
}

type ListFilesReq struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields
}

func (x *ListFilesReq) Reset() {
	*x = ListFilesReq{}
	if protoimpl.UnsafeEnabled {
		mi := &file_engine_proto_datarw_proto_msgTypes[6]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *ListFilesReq) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ListFilesReq) ProtoMessage() {}

func (x *ListFilesReq) ProtoReflect() protoreflect.Message {
	mi := &file_engine_proto_datarw_proto_msgTypes[6]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ListFilesReq.ProtoReflect.Descriptor instead.
func (*ListFilesReq) Descriptor() ([]byte, []int) {
	return file_engine_proto_datarw_proto_rawDescGZIP(), []int{6}
}

type ListFilesResponse struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	FileNum int32 `protobuf:"varint,1,opt,name=fileNum,proto3" json:"fileNum,omitempty"`
}

func (x *ListFilesResponse) Reset() {
	*x = ListFilesResponse{}
	if protoimpl.UnsafeEnabled {
		mi := &file_engine_proto_datarw_proto_msgTypes[7]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *ListFilesResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ListFilesResponse) ProtoMessage() {}

func (x *ListFilesResponse) ProtoReflect() protoreflect.Message {
	mi := &file_engine_proto_datarw_proto_msgTypes[7]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ListFilesResponse.ProtoReflect.Descriptor instead.
func (*ListFilesResponse) Descriptor() ([]byte, []int) {
	return file_engine_proto_datarw_proto_rawDescGZIP(), []int{7}
}

func (x *ListFilesResponse) GetFileNum() int32 {
	if x != nil {
		return x.FileNum
	}
	return 0
}

type ReadLinesRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	FileIdx int32  `protobuf:"varint,1,opt,name=fileIdx,proto3" json:"fileIdx,omitempty"`
	LineNo  []byte `protobuf:"bytes,2,opt,name=lineNo,proto3" json:"lineNo,omitempty"`
}

func (x *ReadLinesRequest) Reset() {
	*x = ReadLinesRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_engine_proto_datarw_proto_msgTypes[8]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *ReadLinesRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ReadLinesRequest) ProtoMessage() {}

func (x *ReadLinesRequest) ProtoReflect() protoreflect.Message {
	mi := &file_engine_proto_datarw_proto_msgTypes[8]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ReadLinesRequest.ProtoReflect.Descriptor instead.
func (*ReadLinesRequest) Descriptor() ([]byte, []int) {
	return file_engine_proto_datarw_proto_rawDescGZIP(), []int{8}
}

func (x *ReadLinesRequest) GetFileIdx() int32 {
	if x != nil {
		return x.FileIdx
	}
	return 0
}

func (x *ReadLinesRequest) GetLineNo() []byte {
	if x != nil {
		return x.LineNo
	}
	return nil
}

type ReadLinesResponse struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Key    []byte `protobuf:"bytes,1,opt,name=key,proto3" json:"key,omitempty"`
	Val    []byte `protobuf:"bytes,2,opt,name=val,proto3" json:"val,omitempty"`
	IsEof  bool   `protobuf:"varint,3,opt,name=isEof,proto3" json:"isEof,omitempty"`
	ErrMsg string `protobuf:"bytes,4,opt,name=errMsg,proto3" json:"errMsg,omitempty"`
}

func (x *ReadLinesResponse) Reset() {
	*x = ReadLinesResponse{}
	if protoimpl.UnsafeEnabled {
		mi := &file_engine_proto_datarw_proto_msgTypes[9]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *ReadLinesResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ReadLinesResponse) ProtoMessage() {}

func (x *ReadLinesResponse) ProtoReflect() protoreflect.Message {
	mi := &file_engine_proto_datarw_proto_msgTypes[9]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ReadLinesResponse.ProtoReflect.Descriptor instead.
func (*ReadLinesResponse) Descriptor() ([]byte, []int) {
	return file_engine_proto_datarw_proto_rawDescGZIP(), []int{9}
}

func (x *ReadLinesResponse) GetKey() []byte {
	if x != nil {
		return x.Key
	}
	return nil
}

func (x *ReadLinesResponse) GetVal() []byte {
	if x != nil {
		return x.Val
	}
	return nil
}

func (x *ReadLinesResponse) GetIsEof() bool {
	if x != nil {
		return x.IsEof
	}
	return false
}

func (x *ReadLinesResponse) GetErrMsg() string {
	if x != nil {
		return x.ErrMsg
	}
	return ""
}

type WriteLinesRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Dir     string `protobuf:"bytes,1,opt,name=dir,proto3" json:"dir,omitempty"`
	FileIdx int32  `protobuf:"varint,2,opt,name=file_idx,json=fileIdx,proto3" json:"file_idx,omitempty"`
	Key     []byte `protobuf:"bytes,3,opt,name=key,proto3" json:"key,omitempty"`
	Value   []byte `protobuf:"bytes,4,opt,name=value,proto3" json:"value,omitempty"`
}

func (x *WriteLinesRequest) Reset() {
	*x = WriteLinesRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_engine_proto_datarw_proto_msgTypes[10]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *WriteLinesRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*WriteLinesRequest) ProtoMessage() {}

func (x *WriteLinesRequest) ProtoReflect() protoreflect.Message {
	mi := &file_engine_proto_datarw_proto_msgTypes[10]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use WriteLinesRequest.ProtoReflect.Descriptor instead.
func (*WriteLinesRequest) Descriptor() ([]byte, []int) {
	return file_engine_proto_datarw_proto_rawDescGZIP(), []int{10}
}

func (x *WriteLinesRequest) GetDir() string {
	if x != nil {
		return x.Dir
	}
	return ""
}

func (x *WriteLinesRequest) GetFileIdx() int32 {
	if x != nil {
		return x.FileIdx
	}
	return 0
}

func (x *WriteLinesRequest) GetKey() []byte {
	if x != nil {
		return x.Key
	}
	return nil
}

func (x *WriteLinesRequest) GetValue() []byte {
	if x != nil {
		return x.Value
	}
	return nil
}

type WriteLinesResponse struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	ErrMsg string `protobuf:"bytes,1,opt,name=err_msg,json=errMsg,proto3" json:"err_msg,omitempty"`
}

func (x *WriteLinesResponse) Reset() {
	*x = WriteLinesResponse{}
	if protoimpl.UnsafeEnabled {
		mi := &file_engine_proto_datarw_proto_msgTypes[11]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *WriteLinesResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*WriteLinesResponse) ProtoMessage() {}

func (x *WriteLinesResponse) ProtoReflect() protoreflect.Message {
	mi := &file_engine_proto_datarw_proto_msgTypes[11]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use WriteLinesResponse.ProtoReflect.Descriptor instead.
func (*WriteLinesResponse) Descriptor() ([]byte, []int) {
	return file_engine_proto_datarw_proto_rawDescGZIP(), []int{11}
}

func (x *WriteLinesResponse) GetErrMsg() string {
	if x != nil {
		return x.ErrMsg
	}
	return ""
}

var File_engine_proto_datarw_proto protoreflect.FileDescriptor

var file_engine_proto_datarw_proto_rawDesc = []byte{
	0x0a, 0x19, 0x65, 0x6e, 0x67, 0x69, 0x6e, 0x65, 0x2f, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x2f, 0x64,
	0x61, 0x74, 0x61, 0x72, 0x77, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12, 0x08, 0x65, 0x6e, 0x67,
	0x69, 0x6e, 0x65, 0x70, 0x62, 0x22, 0x4f, 0x0a, 0x13, 0x47, 0x65, 0x6e, 0x65, 0x72, 0x61, 0x74,
	0x65, 0x44, 0x61, 0x74, 0x61, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x12, 0x19, 0x0a, 0x08,
	0x66, 0x69, 0x6c, 0x65, 0x5f, 0x6e, 0x75, 0x6d, 0x18, 0x01, 0x20, 0x01, 0x28, 0x05, 0x52, 0x07,
	0x66, 0x69, 0x6c, 0x65, 0x4e, 0x75, 0x6d, 0x12, 0x1d, 0x0a, 0x0a, 0x72, 0x65, 0x63, 0x6f, 0x72,
	0x64, 0x5f, 0x6e, 0x75, 0x6d, 0x18, 0x02, 0x20, 0x01, 0x28, 0x05, 0x52, 0x09, 0x72, 0x65, 0x63,
	0x6f, 0x72, 0x64, 0x4e, 0x75, 0x6d, 0x22, 0x2f, 0x0a, 0x14, 0x47, 0x65, 0x6e, 0x65, 0x72, 0x61,
	0x74, 0x65, 0x44, 0x61, 0x74, 0x61, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x12, 0x17,
	0x0a, 0x07, 0x65, 0x72, 0x72, 0x5f, 0x6d, 0x73, 0x67, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52,
	0x06, 0x65, 0x72, 0x72, 0x4d, 0x73, 0x67, 0x22, 0x23, 0x0a, 0x0f, 0x43, 0x68, 0x65, 0x63, 0x6b,
	0x44, 0x69, 0x72, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x12, 0x10, 0x0a, 0x03, 0x64, 0x69,
	0x72, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x03, 0x64, 0x69, 0x72, 0x22, 0x4d, 0x0a, 0x10,
	0x43, 0x68, 0x65, 0x63, 0x6b, 0x44, 0x69, 0x72, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65,
	0x12, 0x17, 0x0a, 0x07, 0x65, 0x72, 0x72, 0x5f, 0x6d, 0x73, 0x67, 0x18, 0x01, 0x20, 0x01, 0x28,
	0x09, 0x52, 0x06, 0x65, 0x72, 0x72, 0x4d, 0x73, 0x67, 0x12, 0x20, 0x0a, 0x0c, 0x65, 0x72, 0x72,
	0x5f, 0x66, 0x69, 0x6c, 0x65, 0x5f, 0x69, 0x64, 0x78, 0x18, 0x02, 0x20, 0x01, 0x28, 0x05, 0x52,
	0x0a, 0x65, 0x72, 0x72, 0x46, 0x69, 0x6c, 0x65, 0x49, 0x64, 0x78, 0x22, 0x10, 0x0a, 0x0e, 0x49,
	0x73, 0x52, 0x65, 0x61, 0x64, 0x79, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x22, 0x27, 0x0a,
	0x0f, 0x49, 0x73, 0x52, 0x65, 0x61, 0x64, 0x79, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65,
	0x12, 0x14, 0x0a, 0x05, 0x72, 0x65, 0x61, 0x64, 0x79, 0x18, 0x01, 0x20, 0x01, 0x28, 0x08, 0x52,
	0x05, 0x72, 0x65, 0x61, 0x64, 0x79, 0x22, 0x0e, 0x0a, 0x0c, 0x4c, 0x69, 0x73, 0x74, 0x46, 0x69,
	0x6c, 0x65, 0x73, 0x52, 0x65, 0x71, 0x22, 0x2d, 0x0a, 0x11, 0x4c, 0x69, 0x73, 0x74, 0x46, 0x69,
	0x6c, 0x65, 0x73, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x12, 0x18, 0x0a, 0x07, 0x66,
	0x69, 0x6c, 0x65, 0x4e, 0x75, 0x6d, 0x18, 0x01, 0x20, 0x01, 0x28, 0x05, 0x52, 0x07, 0x66, 0x69,
	0x6c, 0x65, 0x4e, 0x75, 0x6d, 0x22, 0x44, 0x0a, 0x10, 0x52, 0x65, 0x61, 0x64, 0x4c, 0x69, 0x6e,
	0x65, 0x73, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x12, 0x18, 0x0a, 0x07, 0x66, 0x69, 0x6c,
	0x65, 0x49, 0x64, 0x78, 0x18, 0x01, 0x20, 0x01, 0x28, 0x05, 0x52, 0x07, 0x66, 0x69, 0x6c, 0x65,
	0x49, 0x64, 0x78, 0x12, 0x16, 0x0a, 0x06, 0x6c, 0x69, 0x6e, 0x65, 0x4e, 0x6f, 0x18, 0x02, 0x20,
	0x01, 0x28, 0x0c, 0x52, 0x06, 0x6c, 0x69, 0x6e, 0x65, 0x4e, 0x6f, 0x22, 0x65, 0x0a, 0x11, 0x52,
	0x65, 0x61, 0x64, 0x4c, 0x69, 0x6e, 0x65, 0x73, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65,
	0x12, 0x10, 0x0a, 0x03, 0x6b, 0x65, 0x79, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0c, 0x52, 0x03, 0x6b,
	0x65, 0x79, 0x12, 0x10, 0x0a, 0x03, 0x76, 0x61, 0x6c, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0c, 0x52,
	0x03, 0x76, 0x61, 0x6c, 0x12, 0x14, 0x0a, 0x05, 0x69, 0x73, 0x45, 0x6f, 0x66, 0x18, 0x03, 0x20,
	0x01, 0x28, 0x08, 0x52, 0x05, 0x69, 0x73, 0x45, 0x6f, 0x66, 0x12, 0x16, 0x0a, 0x06, 0x65, 0x72,
	0x72, 0x4d, 0x73, 0x67, 0x18, 0x04, 0x20, 0x01, 0x28, 0x09, 0x52, 0x06, 0x65, 0x72, 0x72, 0x4d,
	0x73, 0x67, 0x22, 0x68, 0x0a, 0x11, 0x57, 0x72, 0x69, 0x74, 0x65, 0x4c, 0x69, 0x6e, 0x65, 0x73,
	0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x12, 0x10, 0x0a, 0x03, 0x64, 0x69, 0x72, 0x18, 0x01,
	0x20, 0x01, 0x28, 0x09, 0x52, 0x03, 0x64, 0x69, 0x72, 0x12, 0x19, 0x0a, 0x08, 0x66, 0x69, 0x6c,
	0x65, 0x5f, 0x69, 0x64, 0x78, 0x18, 0x02, 0x20, 0x01, 0x28, 0x05, 0x52, 0x07, 0x66, 0x69, 0x6c,
	0x65, 0x49, 0x64, 0x78, 0x12, 0x10, 0x0a, 0x03, 0x6b, 0x65, 0x79, 0x18, 0x03, 0x20, 0x01, 0x28,
	0x0c, 0x52, 0x03, 0x6b, 0x65, 0x79, 0x12, 0x14, 0x0a, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x18,
	0x04, 0x20, 0x01, 0x28, 0x0c, 0x52, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x22, 0x2d, 0x0a, 0x12,
	0x57, 0x72, 0x69, 0x74, 0x65, 0x4c, 0x69, 0x6e, 0x65, 0x73, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e,
	0x73, 0x65, 0x12, 0x17, 0x0a, 0x07, 0x65, 0x72, 0x72, 0x5f, 0x6d, 0x73, 0x67, 0x18, 0x01, 0x20,
	0x01, 0x28, 0x09, 0x52, 0x06, 0x65, 0x72, 0x72, 0x4d, 0x73, 0x67, 0x32, 0xba, 0x03, 0x0a, 0x0d,
	0x44, 0x61, 0x74, 0x61, 0x52, 0x57, 0x53, 0x65, 0x72, 0x76, 0x69, 0x63, 0x65, 0x12, 0x48, 0x0a,
	0x09, 0x52, 0x65, 0x61, 0x64, 0x4c, 0x69, 0x6e, 0x65, 0x73, 0x12, 0x1a, 0x2e, 0x65, 0x6e, 0x67,
	0x69, 0x6e, 0x65, 0x70, 0x62, 0x2e, 0x52, 0x65, 0x61, 0x64, 0x4c, 0x69, 0x6e, 0x65, 0x73, 0x52,
	0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x1a, 0x1b, 0x2e, 0x65, 0x6e, 0x67, 0x69, 0x6e, 0x65, 0x70,
	0x62, 0x2e, 0x52, 0x65, 0x61, 0x64, 0x4c, 0x69, 0x6e, 0x65, 0x73, 0x52, 0x65, 0x73, 0x70, 0x6f,
	0x6e, 0x73, 0x65, 0x22, 0x00, 0x30, 0x01, 0x12, 0x4b, 0x0a, 0x0a, 0x57, 0x72, 0x69, 0x74, 0x65,
	0x4c, 0x69, 0x6e, 0x65, 0x73, 0x12, 0x1b, 0x2e, 0x65, 0x6e, 0x67, 0x69, 0x6e, 0x65, 0x70, 0x62,
	0x2e, 0x57, 0x72, 0x69, 0x74, 0x65, 0x4c, 0x69, 0x6e, 0x65, 0x73, 0x52, 0x65, 0x71, 0x75, 0x65,
	0x73, 0x74, 0x1a, 0x1c, 0x2e, 0x65, 0x6e, 0x67, 0x69, 0x6e, 0x65, 0x70, 0x62, 0x2e, 0x57, 0x72,
	0x69, 0x74, 0x65, 0x4c, 0x69, 0x6e, 0x65, 0x73, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65,
	0x22, 0x00, 0x28, 0x01, 0x12, 0x4d, 0x0a, 0x0c, 0x47, 0x65, 0x6e, 0x65, 0x72, 0x61, 0x74, 0x65,
	0x44, 0x61, 0x74, 0x61, 0x12, 0x1d, 0x2e, 0x65, 0x6e, 0x67, 0x69, 0x6e, 0x65, 0x70, 0x62, 0x2e,
	0x47, 0x65, 0x6e, 0x65, 0x72, 0x61, 0x74, 0x65, 0x44, 0x61, 0x74, 0x61, 0x52, 0x65, 0x71, 0x75,
	0x65, 0x73, 0x74, 0x1a, 0x1e, 0x2e, 0x65, 0x6e, 0x67, 0x69, 0x6e, 0x65, 0x70, 0x62, 0x2e, 0x47,
	0x65, 0x6e, 0x65, 0x72, 0x61, 0x74, 0x65, 0x44, 0x61, 0x74, 0x61, 0x52, 0x65, 0x73, 0x70, 0x6f,
	0x6e, 0x73, 0x65, 0x12, 0x40, 0x0a, 0x09, 0x4c, 0x69, 0x73, 0x74, 0x46, 0x69, 0x6c, 0x65, 0x73,
	0x12, 0x16, 0x2e, 0x65, 0x6e, 0x67, 0x69, 0x6e, 0x65, 0x70, 0x62, 0x2e, 0x4c, 0x69, 0x73, 0x74,
	0x46, 0x69, 0x6c, 0x65, 0x73, 0x52, 0x65, 0x71, 0x1a, 0x1b, 0x2e, 0x65, 0x6e, 0x67, 0x69, 0x6e,
	0x65, 0x70, 0x62, 0x2e, 0x4c, 0x69, 0x73, 0x74, 0x46, 0x69, 0x6c, 0x65, 0x73, 0x52, 0x65, 0x73,
	0x70, 0x6f, 0x6e, 0x73, 0x65, 0x12, 0x3e, 0x0a, 0x07, 0x49, 0x73, 0x52, 0x65, 0x61, 0x64, 0x79,
	0x12, 0x18, 0x2e, 0x65, 0x6e, 0x67, 0x69, 0x6e, 0x65, 0x70, 0x62, 0x2e, 0x49, 0x73, 0x52, 0x65,
	0x61, 0x64, 0x79, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x1a, 0x19, 0x2e, 0x65, 0x6e, 0x67,
	0x69, 0x6e, 0x65, 0x70, 0x62, 0x2e, 0x49, 0x73, 0x52, 0x65, 0x61, 0x64, 0x79, 0x52, 0x65, 0x73,
	0x70, 0x6f, 0x6e, 0x73, 0x65, 0x12, 0x41, 0x0a, 0x08, 0x43, 0x68, 0x65, 0x63, 0x6b, 0x44, 0x69,
	0x72, 0x12, 0x19, 0x2e, 0x65, 0x6e, 0x67, 0x69, 0x6e, 0x65, 0x70, 0x62, 0x2e, 0x43, 0x68, 0x65,
	0x63, 0x6b, 0x44, 0x69, 0x72, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x1a, 0x1a, 0x2e, 0x65,
	0x6e, 0x67, 0x69, 0x6e, 0x65, 0x70, 0x62, 0x2e, 0x43, 0x68, 0x65, 0x63, 0x6b, 0x44, 0x69, 0x72,
	0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x42, 0x2b, 0x5a, 0x29, 0x67, 0x69, 0x74, 0x68,
	0x75, 0x62, 0x2e, 0x63, 0x6f, 0x6d, 0x2f, 0x70, 0x69, 0x6e, 0x67, 0x63, 0x61, 0x70, 0x2f, 0x74,
	0x69, 0x66, 0x6c, 0x6f, 0x77, 0x2f, 0x65, 0x6e, 0x67, 0x69, 0x6e, 0x65, 0x2f, 0x65, 0x6e, 0x67,
	0x69, 0x6e, 0x65, 0x70, 0x62, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_engine_proto_datarw_proto_rawDescOnce sync.Once
	file_engine_proto_datarw_proto_rawDescData = file_engine_proto_datarw_proto_rawDesc
)

func file_engine_proto_datarw_proto_rawDescGZIP() []byte {
	file_engine_proto_datarw_proto_rawDescOnce.Do(func() {
		file_engine_proto_datarw_proto_rawDescData = protoimpl.X.CompressGZIP(file_engine_proto_datarw_proto_rawDescData)
	})
	return file_engine_proto_datarw_proto_rawDescData
}

var file_engine_proto_datarw_proto_msgTypes = make([]protoimpl.MessageInfo, 12)
var file_engine_proto_datarw_proto_goTypes = []any{
	(*GenerateDataRequest)(nil),  // 0: enginepb.GenerateDataRequest
	(*GenerateDataResponse)(nil), // 1: enginepb.GenerateDataResponse
	(*CheckDirRequest)(nil),      // 2: enginepb.CheckDirRequest
	(*CheckDirResponse)(nil),     // 3: enginepb.CheckDirResponse
	(*IsReadyRequest)(nil),       // 4: enginepb.IsReadyRequest
	(*IsReadyResponse)(nil),      // 5: enginepb.IsReadyResponse
	(*ListFilesReq)(nil),         // 6: enginepb.ListFilesReq
	(*ListFilesResponse)(nil),    // 7: enginepb.ListFilesResponse
	(*ReadLinesRequest)(nil),     // 8: enginepb.ReadLinesRequest
	(*ReadLinesResponse)(nil),    // 9: enginepb.ReadLinesResponse
	(*WriteLinesRequest)(nil),    // 10: enginepb.WriteLinesRequest
	(*WriteLinesResponse)(nil),   // 11: enginepb.WriteLinesResponse
}
var file_engine_proto_datarw_proto_depIdxs = []int32{
	8,  // 0: enginepb.DataRWService.ReadLines:input_type -> enginepb.ReadLinesRequest
	10, // 1: enginepb.DataRWService.WriteLines:input_type -> enginepb.WriteLinesRequest
	0,  // 2: enginepb.DataRWService.GenerateData:input_type -> enginepb.GenerateDataRequest
	6,  // 3: enginepb.DataRWService.ListFiles:input_type -> enginepb.ListFilesReq
	4,  // 4: enginepb.DataRWService.IsReady:input_type -> enginepb.IsReadyRequest
	2,  // 5: enginepb.DataRWService.CheckDir:input_type -> enginepb.CheckDirRequest
	9,  // 6: enginepb.DataRWService.ReadLines:output_type -> enginepb.ReadLinesResponse
	11, // 7: enginepb.DataRWService.WriteLines:output_type -> enginepb.WriteLinesResponse
	1,  // 8: enginepb.DataRWService.GenerateData:output_type -> enginepb.GenerateDataResponse
	7,  // 9: enginepb.DataRWService.ListFiles:output_type -> enginepb.ListFilesResponse
	5,  // 10: enginepb.DataRWService.IsReady:output_type -> enginepb.IsReadyResponse
	3,  // 11: enginepb.DataRWService.CheckDir:output_type -> enginepb.CheckDirResponse
	6,  // [6:12] is the sub-list for method output_type
	0,  // [0:6] is the sub-list for method input_type
	0,  // [0:0] is the sub-list for extension type_name
	0,  // [0:0] is the sub-list for extension extendee
	0,  // [0:0] is the sub-list for field type_name
}

func init() { file_engine_proto_datarw_proto_init() }
func file_engine_proto_datarw_proto_init() {
	if File_engine_proto_datarw_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_engine_proto_datarw_proto_msgTypes[0].Exporter = func(v any, i int) any {
			switch v := v.(*GenerateDataRequest); i {
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
		file_engine_proto_datarw_proto_msgTypes[1].Exporter = func(v any, i int) any {
			switch v := v.(*GenerateDataResponse); i {
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
		file_engine_proto_datarw_proto_msgTypes[2].Exporter = func(v any, i int) any {
			switch v := v.(*CheckDirRequest); i {
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
		file_engine_proto_datarw_proto_msgTypes[3].Exporter = func(v any, i int) any {
			switch v := v.(*CheckDirResponse); i {
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
		file_engine_proto_datarw_proto_msgTypes[4].Exporter = func(v any, i int) any {
			switch v := v.(*IsReadyRequest); i {
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
		file_engine_proto_datarw_proto_msgTypes[5].Exporter = func(v any, i int) any {
			switch v := v.(*IsReadyResponse); i {
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
		file_engine_proto_datarw_proto_msgTypes[6].Exporter = func(v any, i int) any {
			switch v := v.(*ListFilesReq); i {
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
		file_engine_proto_datarw_proto_msgTypes[7].Exporter = func(v any, i int) any {
			switch v := v.(*ListFilesResponse); i {
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
		file_engine_proto_datarw_proto_msgTypes[8].Exporter = func(v any, i int) any {
			switch v := v.(*ReadLinesRequest); i {
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
		file_engine_proto_datarw_proto_msgTypes[9].Exporter = func(v any, i int) any {
			switch v := v.(*ReadLinesResponse); i {
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
		file_engine_proto_datarw_proto_msgTypes[10].Exporter = func(v any, i int) any {
			switch v := v.(*WriteLinesRequest); i {
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
		file_engine_proto_datarw_proto_msgTypes[11].Exporter = func(v any, i int) any {
			switch v := v.(*WriteLinesResponse); i {
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
			RawDescriptor: file_engine_proto_datarw_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   12,
			NumExtensions: 0,
			NumServices:   1,
		},
		GoTypes:           file_engine_proto_datarw_proto_goTypes,
		DependencyIndexes: file_engine_proto_datarw_proto_depIdxs,
		MessageInfos:      file_engine_proto_datarw_proto_msgTypes,
	}.Build()
	File_engine_proto_datarw_proto = out.File
	file_engine_proto_datarw_proto_rawDesc = nil
	file_engine_proto_datarw_proto_goTypes = nil
	file_engine_proto_datarw_proto_depIdxs = nil
}
