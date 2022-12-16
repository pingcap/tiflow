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

// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.28.0
// 	protoc        v3.20.1
// source: processor/tablepb/table.proto

package tablepb

import (
	_ "github.com/gogo/protobuf/gogoproto"
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

// TableState is the state of table replication in processor.
//
//	┌────────┐   ┌───────────┐   ┌──────────┐
//	│ Absent ├─> │ Preparing ├─> │ Prepared │
//	└────────┘   └───────────┘   └─────┬────┘
//	                                   v
//	┌─────────┐   ┌──────────┐   ┌─────────────┐
//	│ Stopped │ <─┤ Stopping │ <─┤ Replicating │
//	└─────────┘   └──────────┘   └─────────────┘
//
// TODO rename to TableSpanState.
type TableState int32

const (
	TableState_Unknown     TableState = 0
	TableState_Absent      TableState = 1
	TableState_Preparing   TableState = 2
	TableState_Prepared    TableState = 3
	TableState_Replicating TableState = 4
	TableState_Stopping    TableState = 5
	TableState_Stopped     TableState = 6
)

// Enum value maps for TableState.
var (
	TableState_name = map[int32]string{
		0: "Unknown",
		1: "Absent",
		2: "Preparing",
		3: "Prepared",
		4: "Replicating",
		5: "Stopping",
		6: "Stopped",
	}
	TableState_value = map[string]int32{
		"Unknown":     0,
		"Absent":      1,
		"Preparing":   2,
		"Prepared":    3,
		"Replicating": 4,
		"Stopping":    5,
		"Stopped":     6,
	}
)

func (x TableState) Enum() *TableState {
	p := new(TableState)
	*p = x
	return p
}

func (x TableState) String() string {
	return protoimpl.X.EnumStringOf(x.Descriptor(), protoreflect.EnumNumber(x))
}

func (TableState) Descriptor() protoreflect.EnumDescriptor {
	return file_processor_tablepb_table_proto_enumTypes[0].Descriptor()
}

func (TableState) Type() protoreflect.EnumType {
	return &file_processor_tablepb_table_proto_enumTypes[0]
}

func (x TableState) Number() protoreflect.EnumNumber {
	return protoreflect.EnumNumber(x)
}

// Deprecated: Use TableState.Descriptor instead.
func (TableState) EnumDescriptor() ([]byte, []int) {
	return file_processor_tablepb_table_proto_rawDescGZIP(), []int{0}
}

// Span is a full extent of key space from an inclusive start_key to
// an exclusive end_key.
type Span struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	TableId  int64  `protobuf:"varint,1,opt,name=table_id,json=tableId,proto3" json:"table_id,omitempty"`
	StartKey []byte `protobuf:"bytes,2,opt,name=start_key,json=startKey,proto3" json:"start_key,omitempty"`
	EndKey   []byte `protobuf:"bytes,3,opt,name=end_key,json=endKey,proto3" json:"end_key,omitempty"`
}

func (x *Span) Reset() {
	*x = Span{}
	if protoimpl.UnsafeEnabled {
		mi := &file_processor_tablepb_table_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Span) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Span) ProtoMessage() {}

func (x *Span) ProtoReflect() protoreflect.Message {
	mi := &file_processor_tablepb_table_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Span.ProtoReflect.Descriptor instead.
func (*Span) Descriptor() ([]byte, []int) {
	return file_processor_tablepb_table_proto_rawDescGZIP(), []int{0}
}

func (x *Span) GetTableId() int64 {
	if x != nil {
		return x.TableId
	}
	return 0
}

func (x *Span) GetStartKey() []byte {
	if x != nil {
		return x.StartKey
	}
	return nil
}

func (x *Span) GetEndKey() []byte {
	if x != nil {
		return x.EndKey
	}
	return nil
}

type Checkpoint struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	CheckpointTs uint64 `protobuf:"varint,1,opt,name=checkpoint_ts,json=checkpointTs,proto3" json:"checkpoint_ts,omitempty"`
	ResolvedTs   uint64 `protobuf:"varint,2,opt,name=resolved_ts,json=resolvedTs,proto3" json:"resolved_ts,omitempty"`
}

func (x *Checkpoint) Reset() {
	*x = Checkpoint{}
	if protoimpl.UnsafeEnabled {
		mi := &file_processor_tablepb_table_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Checkpoint) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Checkpoint) ProtoMessage() {}

func (x *Checkpoint) ProtoReflect() protoreflect.Message {
	mi := &file_processor_tablepb_table_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Checkpoint.ProtoReflect.Descriptor instead.
func (*Checkpoint) Descriptor() ([]byte, []int) {
	return file_processor_tablepb_table_proto_rawDescGZIP(), []int{1}
}

func (x *Checkpoint) GetCheckpointTs() uint64 {
	if x != nil {
		return x.CheckpointTs
	}
	return 0
}

func (x *Checkpoint) GetResolvedTs() uint64 {
	if x != nil {
		return x.ResolvedTs
	}
	return 0
}

// Stats holds a statistic for a table.
type Stats struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	// Number of captured regions.
	RegionCount uint64 `protobuf:"varint,1,opt,name=region_count,json=regionCount,proto3" json:"region_count,omitempty"`
	// The current timestamp from the table's point of view.
	CurrentTs uint64 `protobuf:"varint,2,opt,name=current_ts,json=currentTs,proto3" json:"current_ts,omitempty"`
	// Checkponits at each stage.
	StageCheckpoints map[string]*Checkpoint `protobuf:"bytes,3,rep,name=stage_checkpoints,json=stageCheckpoints,proto3" json:"stage_checkpoints,omitempty" protobuf_key:"bytes,1,opt,name=key,proto3" protobuf_val:"bytes,2,opt,name=value,proto3"`
	// The barrier timestamp of the table.
	BarrierTs uint64 `protobuf:"varint,4,opt,name=barrier_ts,json=barrierTs,proto3" json:"barrier_ts,omitempty"`
}

func (x *Stats) Reset() {
	*x = Stats{}
	if protoimpl.UnsafeEnabled {
		mi := &file_processor_tablepb_table_proto_msgTypes[2]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Stats) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Stats) ProtoMessage() {}

func (x *Stats) ProtoReflect() protoreflect.Message {
	mi := &file_processor_tablepb_table_proto_msgTypes[2]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Stats.ProtoReflect.Descriptor instead.
func (*Stats) Descriptor() ([]byte, []int) {
	return file_processor_tablepb_table_proto_rawDescGZIP(), []int{2}
}

func (x *Stats) GetRegionCount() uint64 {
	if x != nil {
		return x.RegionCount
	}
	return 0
}

func (x *Stats) GetCurrentTs() uint64 {
	if x != nil {
		return x.CurrentTs
	}
	return 0
}

func (x *Stats) GetStageCheckpoints() map[string]*Checkpoint {
	if x != nil {
		return x.StageCheckpoints
	}
	return nil
}

func (x *Stats) GetBarrierTs() uint64 {
	if x != nil {
		return x.BarrierTs
	}
	return 0
}

// TableStatus is the running status of a table.
// TODO rename to TableStatus.
type TableStatus struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	TableId    int64       `protobuf:"varint,1,opt,name=table_id,json=tableId,proto3" json:"table_id,omitempty"`
	Span       *Span       `protobuf:"bytes,5,opt,name=span,proto3" json:"span,omitempty"`
	State      TableState  `protobuf:"varint,2,opt,name=state,proto3,enum=pingcap.tiflow.cdc.processor.tablepb.TableState" json:"state,omitempty"`
	Checkpoint *Checkpoint `protobuf:"bytes,3,opt,name=checkpoint,proto3" json:"checkpoint,omitempty"`
	Stats      *Stats      `protobuf:"bytes,4,opt,name=stats,proto3" json:"stats,omitempty"`
}

func (x *TableStatus) Reset() {
	*x = TableStatus{}
	if protoimpl.UnsafeEnabled {
		mi := &file_processor_tablepb_table_proto_msgTypes[3]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *TableStatus) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*TableStatus) ProtoMessage() {}

func (x *TableStatus) ProtoReflect() protoreflect.Message {
	mi := &file_processor_tablepb_table_proto_msgTypes[3]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use TableStatus.ProtoReflect.Descriptor instead.
func (*TableStatus) Descriptor() ([]byte, []int) {
	return file_processor_tablepb_table_proto_rawDescGZIP(), []int{3}
}

func (x *TableStatus) GetTableId() int64 {
	if x != nil {
		return x.TableId
	}
	return 0
}

func (x *TableStatus) GetSpan() *Span {
	if x != nil {
		return x.Span
	}
	return nil
}

func (x *TableStatus) GetState() TableState {
	if x != nil {
		return x.State
	}
	return TableState_Unknown
}

func (x *TableStatus) GetCheckpoint() *Checkpoint {
	if x != nil {
		return x.Checkpoint
	}
	return nil
}

func (x *TableStatus) GetStats() *Stats {
	if x != nil {
		return x.Stats
	}
	return nil
}

var File_processor_tablepb_table_proto protoreflect.FileDescriptor

var file_processor_tablepb_table_proto_rawDesc = []byte{
	0x0a, 0x1d, 0x70, 0x72, 0x6f, 0x63, 0x65, 0x73, 0x73, 0x6f, 0x72, 0x2f, 0x74, 0x61, 0x62, 0x6c,
	0x65, 0x70, 0x62, 0x2f, 0x74, 0x61, 0x62, 0x6c, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12,
	0x24, 0x70, 0x69, 0x6e, 0x67, 0x63, 0x61, 0x70, 0x2e, 0x74, 0x69, 0x66, 0x6c, 0x6f, 0x77, 0x2e,
	0x63, 0x64, 0x63, 0x2e, 0x70, 0x72, 0x6f, 0x63, 0x65, 0x73, 0x73, 0x6f, 0x72, 0x2e, 0x74, 0x61,
	0x62, 0x6c, 0x65, 0x70, 0x62, 0x1a, 0x14, 0x67, 0x6f, 0x67, 0x6f, 0x70, 0x72, 0x6f, 0x74, 0x6f,
	0x2f, 0x67, 0x6f, 0x67, 0x6f, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x22, 0xb3, 0x01, 0x0a, 0x04,
	0x53, 0x70, 0x61, 0x6e, 0x12, 0x55, 0x0a, 0x08, 0x74, 0x61, 0x62, 0x6c, 0x65, 0x5f, 0x69, 0x64,
	0x18, 0x01, 0x20, 0x01, 0x28, 0x03, 0x42, 0x3a, 0xfa, 0xde, 0x1f, 0x2b, 0x67, 0x69, 0x74, 0x68,
	0x75, 0x62, 0x2e, 0x63, 0x6f, 0x6d, 0x2f, 0x70, 0x69, 0x6e, 0x67, 0x63, 0x61, 0x70, 0x2f, 0x74,
	0x69, 0x66, 0x6c, 0x6f, 0x77, 0x2f, 0x63, 0x64, 0x63, 0x2f, 0x6d, 0x6f, 0x64, 0x65, 0x6c, 0x2e,
	0x54, 0x61, 0x62, 0x6c, 0x65, 0x49, 0x44, 0xe2, 0xde, 0x1f, 0x07, 0x54, 0x61, 0x62, 0x6c, 0x65,
	0x49, 0x44, 0x52, 0x07, 0x74, 0x61, 0x62, 0x6c, 0x65, 0x49, 0x64, 0x12, 0x24, 0x0a, 0x09, 0x73,
	0x74, 0x61, 0x72, 0x74, 0x5f, 0x6b, 0x65, 0x79, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0c, 0x42, 0x07,
	0xfa, 0xde, 0x1f, 0x03, 0x4b, 0x65, 0x79, 0x52, 0x08, 0x73, 0x74, 0x61, 0x72, 0x74, 0x4b, 0x65,
	0x79, 0x12, 0x20, 0x0a, 0x07, 0x65, 0x6e, 0x64, 0x5f, 0x6b, 0x65, 0x79, 0x18, 0x03, 0x20, 0x01,
	0x28, 0x0c, 0x42, 0x07, 0xfa, 0xde, 0x1f, 0x03, 0x4b, 0x65, 0x79, 0x52, 0x06, 0x65, 0x6e, 0x64,
	0x4b, 0x65, 0x79, 0x3a, 0x0c, 0x80, 0xdc, 0x20, 0x00, 0x98, 0xa0, 0x1f, 0x00, 0x88, 0xa0, 0x1f,
	0x00, 0x22, 0xaa, 0x01, 0x0a, 0x0a, 0x43, 0x68, 0x65, 0x63, 0x6b, 0x70, 0x6f, 0x69, 0x6e, 0x74,
	0x12, 0x4f, 0x0a, 0x0d, 0x63, 0x68, 0x65, 0x63, 0x6b, 0x70, 0x6f, 0x69, 0x6e, 0x74, 0x5f, 0x74,
	0x73, 0x18, 0x01, 0x20, 0x01, 0x28, 0x04, 0x42, 0x2a, 0xfa, 0xde, 0x1f, 0x26, 0x67, 0x69, 0x74,
	0x68, 0x75, 0x62, 0x2e, 0x63, 0x6f, 0x6d, 0x2f, 0x70, 0x69, 0x6e, 0x67, 0x63, 0x61, 0x70, 0x2f,
	0x74, 0x69, 0x66, 0x6c, 0x6f, 0x77, 0x2f, 0x63, 0x64, 0x63, 0x2f, 0x6d, 0x6f, 0x64, 0x65, 0x6c,
	0x2e, 0x54, 0x73, 0x52, 0x0c, 0x63, 0x68, 0x65, 0x63, 0x6b, 0x70, 0x6f, 0x69, 0x6e, 0x74, 0x54,
	0x73, 0x12, 0x4b, 0x0a, 0x0b, 0x72, 0x65, 0x73, 0x6f, 0x6c, 0x76, 0x65, 0x64, 0x5f, 0x74, 0x73,
	0x18, 0x02, 0x20, 0x01, 0x28, 0x04, 0x42, 0x2a, 0xfa, 0xde, 0x1f, 0x26, 0x67, 0x69, 0x74, 0x68,
	0x75, 0x62, 0x2e, 0x63, 0x6f, 0x6d, 0x2f, 0x70, 0x69, 0x6e, 0x67, 0x63, 0x61, 0x70, 0x2f, 0x74,
	0x69, 0x66, 0x6c, 0x6f, 0x77, 0x2f, 0x63, 0x64, 0x63, 0x2f, 0x6d, 0x6f, 0x64, 0x65, 0x6c, 0x2e,
	0x54, 0x73, 0x52, 0x0a, 0x72, 0x65, 0x73, 0x6f, 0x6c, 0x76, 0x65, 0x64, 0x54, 0x73, 0x22, 0xad,
	0x03, 0x0a, 0x05, 0x53, 0x74, 0x61, 0x74, 0x73, 0x12, 0x21, 0x0a, 0x0c, 0x72, 0x65, 0x67, 0x69,
	0x6f, 0x6e, 0x5f, 0x63, 0x6f, 0x75, 0x6e, 0x74, 0x18, 0x01, 0x20, 0x01, 0x28, 0x04, 0x52, 0x0b,
	0x72, 0x65, 0x67, 0x69, 0x6f, 0x6e, 0x43, 0x6f, 0x75, 0x6e, 0x74, 0x12, 0x49, 0x0a, 0x0a, 0x63,
	0x75, 0x72, 0x72, 0x65, 0x6e, 0x74, 0x5f, 0x74, 0x73, 0x18, 0x02, 0x20, 0x01, 0x28, 0x04, 0x42,
	0x2a, 0xfa, 0xde, 0x1f, 0x26, 0x67, 0x69, 0x74, 0x68, 0x75, 0x62, 0x2e, 0x63, 0x6f, 0x6d, 0x2f,
	0x70, 0x69, 0x6e, 0x67, 0x63, 0x61, 0x70, 0x2f, 0x74, 0x69, 0x66, 0x6c, 0x6f, 0x77, 0x2f, 0x63,
	0x64, 0x63, 0x2f, 0x6d, 0x6f, 0x64, 0x65, 0x6c, 0x2e, 0x54, 0x73, 0x52, 0x09, 0x63, 0x75, 0x72,
	0x72, 0x65, 0x6e, 0x74, 0x54, 0x73, 0x12, 0x74, 0x0a, 0x11, 0x73, 0x74, 0x61, 0x67, 0x65, 0x5f,
	0x63, 0x68, 0x65, 0x63, 0x6b, 0x70, 0x6f, 0x69, 0x6e, 0x74, 0x73, 0x18, 0x03, 0x20, 0x03, 0x28,
	0x0b, 0x32, 0x41, 0x2e, 0x70, 0x69, 0x6e, 0x67, 0x63, 0x61, 0x70, 0x2e, 0x74, 0x69, 0x66, 0x6c,
	0x6f, 0x77, 0x2e, 0x63, 0x64, 0x63, 0x2e, 0x70, 0x72, 0x6f, 0x63, 0x65, 0x73, 0x73, 0x6f, 0x72,
	0x2e, 0x74, 0x61, 0x62, 0x6c, 0x65, 0x70, 0x62, 0x2e, 0x53, 0x74, 0x61, 0x74, 0x73, 0x2e, 0x53,
	0x74, 0x61, 0x67, 0x65, 0x43, 0x68, 0x65, 0x63, 0x6b, 0x70, 0x6f, 0x69, 0x6e, 0x74, 0x73, 0x45,
	0x6e, 0x74, 0x72, 0x79, 0x42, 0x04, 0xc8, 0xde, 0x1f, 0x00, 0x52, 0x10, 0x73, 0x74, 0x61, 0x67,
	0x65, 0x43, 0x68, 0x65, 0x63, 0x6b, 0x70, 0x6f, 0x69, 0x6e, 0x74, 0x73, 0x12, 0x49, 0x0a, 0x0a,
	0x62, 0x61, 0x72, 0x72, 0x69, 0x65, 0x72, 0x5f, 0x74, 0x73, 0x18, 0x04, 0x20, 0x01, 0x28, 0x04,
	0x42, 0x2a, 0xfa, 0xde, 0x1f, 0x26, 0x67, 0x69, 0x74, 0x68, 0x75, 0x62, 0x2e, 0x63, 0x6f, 0x6d,
	0x2f, 0x70, 0x69, 0x6e, 0x67, 0x63, 0x61, 0x70, 0x2f, 0x74, 0x69, 0x66, 0x6c, 0x6f, 0x77, 0x2f,
	0x63, 0x64, 0x63, 0x2f, 0x6d, 0x6f, 0x64, 0x65, 0x6c, 0x2e, 0x54, 0x73, 0x52, 0x09, 0x62, 0x61,
	0x72, 0x72, 0x69, 0x65, 0x72, 0x54, 0x73, 0x1a, 0x75, 0x0a, 0x15, 0x53, 0x74, 0x61, 0x67, 0x65,
	0x43, 0x68, 0x65, 0x63, 0x6b, 0x70, 0x6f, 0x69, 0x6e, 0x74, 0x73, 0x45, 0x6e, 0x74, 0x72, 0x79,
	0x12, 0x10, 0x0a, 0x03, 0x6b, 0x65, 0x79, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x03, 0x6b,
	0x65, 0x79, 0x12, 0x46, 0x0a, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x18, 0x02, 0x20, 0x01, 0x28,
	0x0b, 0x32, 0x30, 0x2e, 0x70, 0x69, 0x6e, 0x67, 0x63, 0x61, 0x70, 0x2e, 0x74, 0x69, 0x66, 0x6c,
	0x6f, 0x77, 0x2e, 0x63, 0x64, 0x63, 0x2e, 0x70, 0x72, 0x6f, 0x63, 0x65, 0x73, 0x73, 0x6f, 0x72,
	0x2e, 0x74, 0x61, 0x62, 0x6c, 0x65, 0x70, 0x62, 0x2e, 0x43, 0x68, 0x65, 0x63, 0x6b, 0x70, 0x6f,
	0x69, 0x6e, 0x74, 0x52, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x3a, 0x02, 0x38, 0x01, 0x22, 0x93,
	0x03, 0x0a, 0x0b, 0x54, 0x61, 0x62, 0x6c, 0x65, 0x53, 0x74, 0x61, 0x74, 0x75, 0x73, 0x12, 0x55,
	0x0a, 0x08, 0x74, 0x61, 0x62, 0x6c, 0x65, 0x5f, 0x69, 0x64, 0x18, 0x01, 0x20, 0x01, 0x28, 0x03,
	0x42, 0x3a, 0xfa, 0xde, 0x1f, 0x2b, 0x67, 0x69, 0x74, 0x68, 0x75, 0x62, 0x2e, 0x63, 0x6f, 0x6d,
	0x2f, 0x70, 0x69, 0x6e, 0x67, 0x63, 0x61, 0x70, 0x2f, 0x74, 0x69, 0x66, 0x6c, 0x6f, 0x77, 0x2f,
	0x63, 0x64, 0x63, 0x2f, 0x6d, 0x6f, 0x64, 0x65, 0x6c, 0x2e, 0x54, 0x61, 0x62, 0x6c, 0x65, 0x49,
	0x44, 0xe2, 0xde, 0x1f, 0x07, 0x54, 0x61, 0x62, 0x6c, 0x65, 0x49, 0x44, 0x52, 0x07, 0x74, 0x61,
	0x62, 0x6c, 0x65, 0x49, 0x64, 0x12, 0x44, 0x0a, 0x04, 0x73, 0x70, 0x61, 0x6e, 0x18, 0x05, 0x20,
	0x01, 0x28, 0x0b, 0x32, 0x2a, 0x2e, 0x70, 0x69, 0x6e, 0x67, 0x63, 0x61, 0x70, 0x2e, 0x74, 0x69,
	0x66, 0x6c, 0x6f, 0x77, 0x2e, 0x63, 0x64, 0x63, 0x2e, 0x70, 0x72, 0x6f, 0x63, 0x65, 0x73, 0x73,
	0x6f, 0x72, 0x2e, 0x74, 0x61, 0x62, 0x6c, 0x65, 0x70, 0x62, 0x2e, 0x53, 0x70, 0x61, 0x6e, 0x42,
	0x04, 0xc8, 0xde, 0x1f, 0x00, 0x52, 0x04, 0x73, 0x70, 0x61, 0x6e, 0x12, 0x46, 0x0a, 0x05, 0x73,
	0x74, 0x61, 0x74, 0x65, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0e, 0x32, 0x30, 0x2e, 0x70, 0x69, 0x6e,
	0x67, 0x63, 0x61, 0x70, 0x2e, 0x74, 0x69, 0x66, 0x6c, 0x6f, 0x77, 0x2e, 0x63, 0x64, 0x63, 0x2e,
	0x70, 0x72, 0x6f, 0x63, 0x65, 0x73, 0x73, 0x6f, 0x72, 0x2e, 0x74, 0x61, 0x62, 0x6c, 0x65, 0x70,
	0x62, 0x2e, 0x54, 0x61, 0x62, 0x6c, 0x65, 0x53, 0x74, 0x61, 0x74, 0x65, 0x52, 0x05, 0x73, 0x74,
	0x61, 0x74, 0x65, 0x12, 0x56, 0x0a, 0x0a, 0x63, 0x68, 0x65, 0x63, 0x6b, 0x70, 0x6f, 0x69, 0x6e,
	0x74, 0x18, 0x03, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x30, 0x2e, 0x70, 0x69, 0x6e, 0x67, 0x63, 0x61,
	0x70, 0x2e, 0x74, 0x69, 0x66, 0x6c, 0x6f, 0x77, 0x2e, 0x63, 0x64, 0x63, 0x2e, 0x70, 0x72, 0x6f,
	0x63, 0x65, 0x73, 0x73, 0x6f, 0x72, 0x2e, 0x74, 0x61, 0x62, 0x6c, 0x65, 0x70, 0x62, 0x2e, 0x43,
	0x68, 0x65, 0x63, 0x6b, 0x70, 0x6f, 0x69, 0x6e, 0x74, 0x42, 0x04, 0xc8, 0xde, 0x1f, 0x00, 0x52,
	0x0a, 0x63, 0x68, 0x65, 0x63, 0x6b, 0x70, 0x6f, 0x69, 0x6e, 0x74, 0x12, 0x47, 0x0a, 0x05, 0x73,
	0x74, 0x61, 0x74, 0x73, 0x18, 0x04, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x2b, 0x2e, 0x70, 0x69, 0x6e,
	0x67, 0x63, 0x61, 0x70, 0x2e, 0x74, 0x69, 0x66, 0x6c, 0x6f, 0x77, 0x2e, 0x63, 0x64, 0x63, 0x2e,
	0x70, 0x72, 0x6f, 0x63, 0x65, 0x73, 0x73, 0x6f, 0x72, 0x2e, 0x74, 0x61, 0x62, 0x6c, 0x65, 0x70,
	0x62, 0x2e, 0x53, 0x74, 0x61, 0x74, 0x73, 0x42, 0x04, 0xc8, 0xde, 0x1f, 0x00, 0x52, 0x05, 0x73,
	0x74, 0x61, 0x74, 0x73, 0x2a, 0x96, 0x02, 0x0a, 0x0a, 0x54, 0x61, 0x62, 0x6c, 0x65, 0x53, 0x74,
	0x61, 0x74, 0x65, 0x12, 0x22, 0x0a, 0x07, 0x55, 0x6e, 0x6b, 0x6e, 0x6f, 0x77, 0x6e, 0x10, 0x00,
	0x1a, 0x15, 0x8a, 0x9d, 0x20, 0x11, 0x54, 0x61, 0x62, 0x6c, 0x65, 0x53, 0x74, 0x61, 0x74, 0x65,
	0x55, 0x6e, 0x6b, 0x6e, 0x6f, 0x77, 0x6e, 0x12, 0x20, 0x0a, 0x06, 0x41, 0x62, 0x73, 0x65, 0x6e,
	0x74, 0x10, 0x01, 0x1a, 0x14, 0x8a, 0x9d, 0x20, 0x10, 0x54, 0x61, 0x62, 0x6c, 0x65, 0x53, 0x74,
	0x61, 0x74, 0x65, 0x41, 0x62, 0x73, 0x65, 0x6e, 0x74, 0x12, 0x26, 0x0a, 0x09, 0x50, 0x72, 0x65,
	0x70, 0x61, 0x72, 0x69, 0x6e, 0x67, 0x10, 0x02, 0x1a, 0x17, 0x8a, 0x9d, 0x20, 0x13, 0x54, 0x61,
	0x62, 0x6c, 0x65, 0x53, 0x74, 0x61, 0x74, 0x65, 0x50, 0x72, 0x65, 0x70, 0x61, 0x72, 0x69, 0x6e,
	0x67, 0x12, 0x24, 0x0a, 0x08, 0x50, 0x72, 0x65, 0x70, 0x61, 0x72, 0x65, 0x64, 0x10, 0x03, 0x1a,
	0x16, 0x8a, 0x9d, 0x20, 0x12, 0x54, 0x61, 0x62, 0x6c, 0x65, 0x53, 0x74, 0x61, 0x74, 0x65, 0x50,
	0x72, 0x65, 0x70, 0x61, 0x72, 0x65, 0x64, 0x12, 0x2a, 0x0a, 0x0b, 0x52, 0x65, 0x70, 0x6c, 0x69,
	0x63, 0x61, 0x74, 0x69, 0x6e, 0x67, 0x10, 0x04, 0x1a, 0x19, 0x8a, 0x9d, 0x20, 0x15, 0x54, 0x61,
	0x62, 0x6c, 0x65, 0x53, 0x74, 0x61, 0x74, 0x65, 0x52, 0x65, 0x70, 0x6c, 0x69, 0x63, 0x61, 0x74,
	0x69, 0x6e, 0x67, 0x12, 0x24, 0x0a, 0x08, 0x53, 0x74, 0x6f, 0x70, 0x70, 0x69, 0x6e, 0x67, 0x10,
	0x05, 0x1a, 0x16, 0x8a, 0x9d, 0x20, 0x12, 0x54, 0x61, 0x62, 0x6c, 0x65, 0x53, 0x74, 0x61, 0x74,
	0x65, 0x53, 0x74, 0x6f, 0x70, 0x70, 0x69, 0x6e, 0x67, 0x12, 0x22, 0x0a, 0x07, 0x53, 0x74, 0x6f,
	0x70, 0x70, 0x65, 0x64, 0x10, 0x06, 0x1a, 0x15, 0x8a, 0x9d, 0x20, 0x11, 0x54, 0x61, 0x62, 0x6c,
	0x65, 0x53, 0x74, 0x61, 0x74, 0x65, 0x53, 0x74, 0x6f, 0x70, 0x70, 0x65, 0x64, 0x42, 0x35, 0x5a,
	0x2f, 0x67, 0x69, 0x74, 0x68, 0x75, 0x62, 0x2e, 0x63, 0x6f, 0x6d, 0x2f, 0x70, 0x69, 0x6e, 0x67,
	0x63, 0x61, 0x70, 0x2f, 0x74, 0x69, 0x66, 0x6c, 0x6f, 0x77, 0x2f, 0x63, 0x64, 0x63, 0x2f, 0x70,
	0x72, 0x6f, 0x63, 0x65, 0x73, 0x73, 0x6f, 0x72, 0x2f, 0x74, 0x61, 0x62, 0x6c, 0x65, 0x70, 0x62,
	0xd0, 0xe1, 0x1e, 0x00, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_processor_tablepb_table_proto_rawDescOnce sync.Once
	file_processor_tablepb_table_proto_rawDescData = file_processor_tablepb_table_proto_rawDesc
)

func file_processor_tablepb_table_proto_rawDescGZIP() []byte {
	file_processor_tablepb_table_proto_rawDescOnce.Do(func() {
		file_processor_tablepb_table_proto_rawDescData = protoimpl.X.CompressGZIP(file_processor_tablepb_table_proto_rawDescData)
	})
	return file_processor_tablepb_table_proto_rawDescData
}

var file_processor_tablepb_table_proto_enumTypes = make([]protoimpl.EnumInfo, 1)
var file_processor_tablepb_table_proto_msgTypes = make([]protoimpl.MessageInfo, 5)
var file_processor_tablepb_table_proto_goTypes = []interface{}{
	(TableState)(0),     // 0: pingcap.tiflow.cdc.processor.tablepb.TableState
	(*Span)(nil),        // 1: pingcap.tiflow.cdc.processor.tablepb.Span
	(*Checkpoint)(nil),  // 2: pingcap.tiflow.cdc.processor.tablepb.Checkpoint
	(*Stats)(nil),       // 3: pingcap.tiflow.cdc.processor.tablepb.Stats
	(*TableStatus)(nil), // 4: pingcap.tiflow.cdc.processor.tablepb.TableStatus
	nil,                 // 5: pingcap.tiflow.cdc.processor.tablepb.Stats.StageCheckpointsEntry
}
var file_processor_tablepb_table_proto_depIdxs = []int32{
	5, // 0: pingcap.tiflow.cdc.processor.tablepb.Stats.stage_checkpoints:type_name -> pingcap.tiflow.cdc.processor.tablepb.Stats.StageCheckpointsEntry
	1, // 1: pingcap.tiflow.cdc.processor.tablepb.TableStatus.span:type_name -> pingcap.tiflow.cdc.processor.tablepb.Span
	0, // 2: pingcap.tiflow.cdc.processor.tablepb.TableStatus.state:type_name -> pingcap.tiflow.cdc.processor.tablepb.TableState
	2, // 3: pingcap.tiflow.cdc.processor.tablepb.TableStatus.checkpoint:type_name -> pingcap.tiflow.cdc.processor.tablepb.Checkpoint
	3, // 4: pingcap.tiflow.cdc.processor.tablepb.TableStatus.stats:type_name -> pingcap.tiflow.cdc.processor.tablepb.Stats
	2, // 5: pingcap.tiflow.cdc.processor.tablepb.Stats.StageCheckpointsEntry.value:type_name -> pingcap.tiflow.cdc.processor.tablepb.Checkpoint
	6, // [6:6] is the sub-list for method output_type
	6, // [6:6] is the sub-list for method input_type
	6, // [6:6] is the sub-list for extension type_name
	6, // [6:6] is the sub-list for extension extendee
	0, // [0:6] is the sub-list for field type_name
}

func init() { file_processor_tablepb_table_proto_init() }
func file_processor_tablepb_table_proto_init() {
	if File_processor_tablepb_table_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_processor_tablepb_table_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*Span); i {
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
		file_processor_tablepb_table_proto_msgTypes[1].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*Checkpoint); i {
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
		file_processor_tablepb_table_proto_msgTypes[2].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*Stats); i {
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
		file_processor_tablepb_table_proto_msgTypes[3].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*TableStatus); i {
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
			RawDescriptor: file_processor_tablepb_table_proto_rawDesc,
			NumEnums:      1,
			NumMessages:   5,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_processor_tablepb_table_proto_goTypes,
		DependencyIndexes: file_processor_tablepb_table_proto_depIdxs,
		EnumInfos:         file_processor_tablepb_table_proto_enumTypes,
		MessageInfos:      file_processor_tablepb_table_proto_msgTypes,
	}.Build()
	File_processor_tablepb_table_proto = out.File
	file_processor_tablepb_table_proto_rawDesc = nil
	file_processor_tablepb_table_proto_goTypes = nil
	file_processor_tablepb_table_proto_depIdxs = nil
}
