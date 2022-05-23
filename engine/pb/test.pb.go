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

// Code generated by protoc-gen-gogo. DO NOT EDIT.
// source: test.proto

package pb

import (
	context "context"
	fmt "fmt"
	io "io"
	math "math"
	math_bits "math/bits"

	proto "github.com/gogo/protobuf/proto"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
)

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

// This is a compile-time assertion to ensure that this generated file
// is compatible with the proto package it is being compiled against.
// A compilation error at this line likely means your copy of the
// proto package needs to be updated.
const _ = proto.GoGoProtoPackageIsVersion3 // please upgrade the proto package

type Record_RecordType int32

const (
	Record_Data Record_RecordType = 0
	Record_DDL  Record_RecordType = 1
)

var Record_RecordType_name = map[int32]string{
	0: "Data",
	1: "DDL",
}

var Record_RecordType_value = map[string]int32{
	"Data": 0,
	"DDL":  1,
}

func (x Record_RecordType) String() string {
	return proto.EnumName(Record_RecordType_name, int32(x))
}

func (Record_RecordType) EnumDescriptor() ([]byte, []int) {
	return fileDescriptor_c161fcfdc0c3ff1e, []int{0, 0}
}

type Record struct {
	Tp        Record_RecordType `protobuf:"varint,1,opt,name=tp,proto3,enum=pb.Record_RecordType" json:"tp,omitempty"`
	SchemaVer int32             `protobuf:"varint,2,opt,name=schema_ver,json=schemaVer,proto3" json:"schema_ver,omitempty"`
	Tid       int32             `protobuf:"varint,3,opt,name=tid,proto3" json:"tid,omitempty"`
	Gtid      int32             `protobuf:"varint,4,opt,name=gtid,proto3" json:"gtid,omitempty"`
	Pk        int32             `protobuf:"varint,5,opt,name=pk,proto3" json:"pk,omitempty"`
	// for record time
	TimeTracer []int64 `protobuf:"varint,6,rep,packed,name=time_tracer,json=timeTracer,proto3" json:"time_tracer,omitempty"`
	// error
	Err *Error `protobuf:"bytes,7,opt,name=err,proto3" json:"err,omitempty"`
}

func (m *Record) Reset()         { *m = Record{} }
func (m *Record) String() string { return proto.CompactTextString(m) }
func (*Record) ProtoMessage()    {}
func (*Record) Descriptor() ([]byte, []int) {
	return fileDescriptor_c161fcfdc0c3ff1e, []int{0}
}
func (m *Record) XXX_Unmarshal(b []byte) error {
	return m.Unmarshal(b)
}
func (m *Record) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	if deterministic {
		return xxx_messageInfo_Record.Marshal(b, m, deterministic)
	} else {
		b = b[:cap(b)]
		n, err := m.MarshalToSizedBuffer(b)
		if err != nil {
			return nil, err
		}
		return b[:n], nil
	}
}
func (m *Record) XXX_Merge(src proto.Message) {
	xxx_messageInfo_Record.Merge(m, src)
}
func (m *Record) XXX_Size() int {
	return m.Size()
}
func (m *Record) XXX_DiscardUnknown() {
	xxx_messageInfo_Record.DiscardUnknown(m)
}

var xxx_messageInfo_Record proto.InternalMessageInfo

func (m *Record) GetTp() Record_RecordType {
	if m != nil {
		return m.Tp
	}
	return Record_Data
}

func (m *Record) GetSchemaVer() int32 {
	if m != nil {
		return m.SchemaVer
	}
	return 0
}

func (m *Record) GetTid() int32 {
	if m != nil {
		return m.Tid
	}
	return 0
}

func (m *Record) GetGtid() int32 {
	if m != nil {
		return m.Gtid
	}
	return 0
}

func (m *Record) GetPk() int32 {
	if m != nil {
		return m.Pk
	}
	return 0
}

func (m *Record) GetTimeTracer() []int64 {
	if m != nil {
		return m.TimeTracer
	}
	return nil
}

func (m *Record) GetErr() *Error {
	if m != nil {
		return m.Err
	}
	return nil
}

type TestBinlogRequest struct {
	Gtid int32 `protobuf:"varint,1,opt,name=gtid,proto3" json:"gtid,omitempty"`
}

func (m *TestBinlogRequest) Reset()         { *m = TestBinlogRequest{} }
func (m *TestBinlogRequest) String() string { return proto.CompactTextString(m) }
func (*TestBinlogRequest) ProtoMessage()    {}
func (*TestBinlogRequest) Descriptor() ([]byte, []int) {
	return fileDescriptor_c161fcfdc0c3ff1e, []int{1}
}
func (m *TestBinlogRequest) XXX_Unmarshal(b []byte) error {
	return m.Unmarshal(b)
}
func (m *TestBinlogRequest) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	if deterministic {
		return xxx_messageInfo_TestBinlogRequest.Marshal(b, m, deterministic)
	} else {
		b = b[:cap(b)]
		n, err := m.MarshalToSizedBuffer(b)
		if err != nil {
			return nil, err
		}
		return b[:n], nil
	}
}
func (m *TestBinlogRequest) XXX_Merge(src proto.Message) {
	xxx_messageInfo_TestBinlogRequest.Merge(m, src)
}
func (m *TestBinlogRequest) XXX_Size() int {
	return m.Size()
}
func (m *TestBinlogRequest) XXX_DiscardUnknown() {
	xxx_messageInfo_TestBinlogRequest.DiscardUnknown(m)
}

var xxx_messageInfo_TestBinlogRequest proto.InternalMessageInfo

func (m *TestBinlogRequest) GetGtid() int32 {
	if m != nil {
		return m.Gtid
	}
	return 0
}

func init() {
	proto.RegisterEnum("pb.Record_RecordType", Record_RecordType_name, Record_RecordType_value)
	proto.RegisterType((*Record)(nil), "pb.Record")
	proto.RegisterType((*TestBinlogRequest)(nil), "pb.TestBinlogRequest")
}

func init() { proto.RegisterFile("test.proto", fileDescriptor_c161fcfdc0c3ff1e) }

var fileDescriptor_c161fcfdc0c3ff1e = []byte{
	// 305 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0x64, 0xd0, 0xcf, 0x4a, 0xc3, 0x40,
	0x10, 0x06, 0xf0, 0x6c, 0xd2, 0x3f, 0x76, 0x02, 0xa5, 0x0e, 0x08, 0x4b, 0xc5, 0x34, 0x14, 0xc4,
	0x9c, 0x8a, 0xd6, 0x17, 0x90, 0x52, 0x3d, 0x79, 0x5a, 0x8b, 0xd7, 0x92, 0xa6, 0x43, 0x0d, 0xb5,
	0x66, 0x9d, 0xac, 0x05, 0xdf, 0xc2, 0xc7, 0xf2, 0xd8, 0xa3, 0x47, 0x69, 0x5e, 0x44, 0x76, 0x23,
	0xf6, 0xe0, 0x69, 0x87, 0x1f, 0xbb, 0xcb, 0x37, 0x1f, 0x80, 0xa1, 0xd2, 0x8c, 0x34, 0x17, 0xa6,
	0x40, 0x5f, 0x2f, 0xfa, 0x21, 0x31, 0x17, 0x5c, 0xc3, 0xb0, 0x12, 0xd0, 0x52, 0x94, 0x15, 0xbc,
	0xc4, 0x73, 0xf0, 0x8d, 0x96, 0x22, 0x16, 0x49, 0x77, 0x7c, 0x32, 0xd2, 0x8b, 0x51, 0xed, 0xbf,
	0xc7, 0xec, 0x5d, 0x93, 0xf2, 0x8d, 0xc6, 0x33, 0x80, 0x32, 0x7b, 0xa2, 0x4d, 0x3a, 0xdf, 0x12,
	0x4b, 0x3f, 0x16, 0x49, 0x53, 0x75, 0x6a, 0x79, 0x24, 0xc6, 0x1e, 0x04, 0x26, 0x5f, 0xca, 0xc0,
	0xb9, 0x1d, 0x11, 0xa1, 0xb1, 0xb2, 0xd4, 0x70, 0xe4, 0x66, 0xec, 0x82, 0xaf, 0xd7, 0xb2, 0xe9,
	0xc4, 0xd7, 0x6b, 0x1c, 0x40, 0x68, 0xf2, 0x0d, 0xcd, 0x0d, 0xa7, 0x19, 0xb1, 0x6c, 0xc5, 0x41,
	0x12, 0x28, 0xb0, 0x34, 0x73, 0x82, 0xa7, 0x10, 0x10, 0xb3, 0x6c, 0xc7, 0x22, 0x09, 0xc7, 0x1d,
	0x9b, 0xee, 0xd6, 0x6e, 0xa1, 0xac, 0x0e, 0x07, 0x00, 0x87, 0x90, 0x78, 0x04, 0x8d, 0x69, 0x6a,
	0xd2, 0x9e, 0x87, 0x6d, 0x08, 0xa6, 0xd3, 0xfb, 0x9e, 0x18, 0x5e, 0xc0, 0xf1, 0x8c, 0x4a, 0x33,
	0xc9, 0x5f, 0x9e, 0x8b, 0x95, 0xa2, 0xd7, 0x37, 0x2a, 0xcd, 0x5f, 0x2e, 0x71, 0xc8, 0x35, 0xbe,
	0x81, 0xd0, 0x5e, 0x7c, 0x20, 0xde, 0xe6, 0x19, 0xe1, 0x15, 0xc0, 0x1d, 0xd1, 0xb2, 0x7e, 0x87,
	0xae, 0x94, 0x7f, 0xff, 0xf4, 0xe1, 0xd0, 0xd5, 0xa5, 0x98, 0xc8, 0xcf, 0x7d, 0x24, 0x76, 0xfb,
	0x48, 0x7c, 0xef, 0x23, 0xf1, 0x51, 0x45, 0xde, 0xae, 0x8a, 0xbc, 0xaf, 0x2a, 0xf2, 0x16, 0x2d,
	0xd7, 0xf8, 0xf5, 0x4f, 0x00, 0x00, 0x00, 0xff, 0xff, 0x27, 0x14, 0xe3, 0x72, 0x90, 0x01, 0x00,
	0x00,
}

// Reference imports to suppress errors if they are not otherwise used.
var _ context.Context
var _ grpc.ClientConn

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
const _ = grpc.SupportPackageIsVersion4

// TestServiceClient is the client API for TestService service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://godoc.org/google.golang.org/grpc#ClientConn.NewStream.
type TestServiceClient interface {
	FeedBinlog(ctx context.Context, in *TestBinlogRequest, opts ...grpc.CallOption) (TestService_FeedBinlogClient, error)
}

type testServiceClient struct {
	cc *grpc.ClientConn
}

func NewTestServiceClient(cc *grpc.ClientConn) TestServiceClient {
	return &testServiceClient{cc}
}

func (c *testServiceClient) FeedBinlog(ctx context.Context, in *TestBinlogRequest, opts ...grpc.CallOption) (TestService_FeedBinlogClient, error) {
	stream, err := c.cc.NewStream(ctx, &_TestService_serviceDesc.Streams[0], "/pb.TestService/FeedBinlog", opts...)
	if err != nil {
		return nil, err
	}
	x := &testServiceFeedBinlogClient{stream}
	if err := x.ClientStream.SendMsg(in); err != nil {
		return nil, err
	}
	if err := x.ClientStream.CloseSend(); err != nil {
		return nil, err
	}
	return x, nil
}

type TestService_FeedBinlogClient interface {
	Recv() (*Record, error)
	grpc.ClientStream
}

type testServiceFeedBinlogClient struct {
	grpc.ClientStream
}

func (x *testServiceFeedBinlogClient) Recv() (*Record, error) {
	m := new(Record)
	if err := x.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

// TestServiceServer is the server API for TestService service.
type TestServiceServer interface {
	FeedBinlog(*TestBinlogRequest, TestService_FeedBinlogServer) error
}

// UnimplementedTestServiceServer can be embedded to have forward compatible implementations.
type UnimplementedTestServiceServer struct {
}

func (*UnimplementedTestServiceServer) FeedBinlog(req *TestBinlogRequest, srv TestService_FeedBinlogServer) error {
	return status.Errorf(codes.Unimplemented, "method FeedBinlog not implemented")
}

func RegisterTestServiceServer(s *grpc.Server, srv TestServiceServer) {
	s.RegisterService(&_TestService_serviceDesc, srv)
}

func _TestService_FeedBinlog_Handler(srv interface{}, stream grpc.ServerStream) error {
	m := new(TestBinlogRequest)
	if err := stream.RecvMsg(m); err != nil {
		return err
	}
	return srv.(TestServiceServer).FeedBinlog(m, &testServiceFeedBinlogServer{stream})
}

type TestService_FeedBinlogServer interface {
	Send(*Record) error
	grpc.ServerStream
}

type testServiceFeedBinlogServer struct {
	grpc.ServerStream
}

func (x *testServiceFeedBinlogServer) Send(m *Record) error {
	return x.ServerStream.SendMsg(m)
}

var _TestService_serviceDesc = grpc.ServiceDesc{
	ServiceName: "pb.TestService",
	HandlerType: (*TestServiceServer)(nil),
	Methods:     []grpc.MethodDesc{},
	Streams: []grpc.StreamDesc{
		{
			StreamName:    "FeedBinlog",
			Handler:       _TestService_FeedBinlog_Handler,
			ServerStreams: true,
		},
	},
	Metadata: "test.proto",
}

func (m *Record) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalToSizedBuffer(dAtA[:size])
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *Record) MarshalTo(dAtA []byte) (int, error) {
	size := m.Size()
	return m.MarshalToSizedBuffer(dAtA[:size])
}

func (m *Record) MarshalToSizedBuffer(dAtA []byte) (int, error) {
	i := len(dAtA)
	_ = i
	var l int
	_ = l
	if m.Err != nil {
		{
			size, err := m.Err.MarshalToSizedBuffer(dAtA[:i])
			if err != nil {
				return 0, err
			}
			i -= size
			i = encodeVarintTest(dAtA, i, uint64(size))
		}
		i--
		dAtA[i] = 0x3a
	}
	if len(m.TimeTracer) > 0 {
		dAtA3 := make([]byte, len(m.TimeTracer)*10)
		var j2 int
		for _, num1 := range m.TimeTracer {
			num := uint64(num1)
			for num >= 1<<7 {
				dAtA3[j2] = uint8(uint64(num)&0x7f | 0x80)
				num >>= 7
				j2++
			}
			dAtA3[j2] = uint8(num)
			j2++
		}
		i -= j2
		copy(dAtA[i:], dAtA3[:j2])
		i = encodeVarintTest(dAtA, i, uint64(j2))
		i--
		dAtA[i] = 0x32
	}
	if m.Pk != 0 {
		i = encodeVarintTest(dAtA, i, uint64(m.Pk))
		i--
		dAtA[i] = 0x28
	}
	if m.Gtid != 0 {
		i = encodeVarintTest(dAtA, i, uint64(m.Gtid))
		i--
		dAtA[i] = 0x20
	}
	if m.Tid != 0 {
		i = encodeVarintTest(dAtA, i, uint64(m.Tid))
		i--
		dAtA[i] = 0x18
	}
	if m.SchemaVer != 0 {
		i = encodeVarintTest(dAtA, i, uint64(m.SchemaVer))
		i--
		dAtA[i] = 0x10
	}
	if m.Tp != 0 {
		i = encodeVarintTest(dAtA, i, uint64(m.Tp))
		i--
		dAtA[i] = 0x8
	}
	return len(dAtA) - i, nil
}

func (m *TestBinlogRequest) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalToSizedBuffer(dAtA[:size])
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *TestBinlogRequest) MarshalTo(dAtA []byte) (int, error) {
	size := m.Size()
	return m.MarshalToSizedBuffer(dAtA[:size])
}

func (m *TestBinlogRequest) MarshalToSizedBuffer(dAtA []byte) (int, error) {
	i := len(dAtA)
	_ = i
	var l int
	_ = l
	if m.Gtid != 0 {
		i = encodeVarintTest(dAtA, i, uint64(m.Gtid))
		i--
		dAtA[i] = 0x8
	}
	return len(dAtA) - i, nil
}

func encodeVarintTest(dAtA []byte, offset int, v uint64) int {
	offset -= sovTest(v)
	base := offset
	for v >= 1<<7 {
		dAtA[offset] = uint8(v&0x7f | 0x80)
		v >>= 7
		offset++
	}
	dAtA[offset] = uint8(v)
	return base
}
func (m *Record) Size() (n int) {
	if m == nil {
		return 0
	}
	var l int
	_ = l
	if m.Tp != 0 {
		n += 1 + sovTest(uint64(m.Tp))
	}
	if m.SchemaVer != 0 {
		n += 1 + sovTest(uint64(m.SchemaVer))
	}
	if m.Tid != 0 {
		n += 1 + sovTest(uint64(m.Tid))
	}
	if m.Gtid != 0 {
		n += 1 + sovTest(uint64(m.Gtid))
	}
	if m.Pk != 0 {
		n += 1 + sovTest(uint64(m.Pk))
	}
	if len(m.TimeTracer) > 0 {
		l = 0
		for _, e := range m.TimeTracer {
			l += sovTest(uint64(e))
		}
		n += 1 + sovTest(uint64(l)) + l
	}
	if m.Err != nil {
		l = m.Err.Size()
		n += 1 + l + sovTest(uint64(l))
	}
	return n
}

func (m *TestBinlogRequest) Size() (n int) {
	if m == nil {
		return 0
	}
	var l int
	_ = l
	if m.Gtid != 0 {
		n += 1 + sovTest(uint64(m.Gtid))
	}
	return n
}

func sovTest(x uint64) (n int) {
	return (math_bits.Len64(x|1) + 6) / 7
}
func sozTest(x uint64) (n int) {
	return sovTest(uint64((x << 1) ^ uint64((int64(x) >> 63))))
}
func (m *Record) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowTest
			}
			if iNdEx >= l {
				return io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		if wireType == 4 {
			return fmt.Errorf("proto: Record: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: Record: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field Tp", wireType)
			}
			m.Tp = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowTest
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				m.Tp |= Record_RecordType(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
		case 2:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field SchemaVer", wireType)
			}
			m.SchemaVer = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowTest
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				m.SchemaVer |= int32(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
		case 3:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field Tid", wireType)
			}
			m.Tid = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowTest
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				m.Tid |= int32(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
		case 4:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field Gtid", wireType)
			}
			m.Gtid = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowTest
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				m.Gtid |= int32(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
		case 5:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field Pk", wireType)
			}
			m.Pk = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowTest
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				m.Pk |= int32(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
		case 6:
			if wireType == 0 {
				var v int64
				for shift := uint(0); ; shift += 7 {
					if shift >= 64 {
						return ErrIntOverflowTest
					}
					if iNdEx >= l {
						return io.ErrUnexpectedEOF
					}
					b := dAtA[iNdEx]
					iNdEx++
					v |= int64(b&0x7F) << shift
					if b < 0x80 {
						break
					}
				}
				m.TimeTracer = append(m.TimeTracer, v)
			} else if wireType == 2 {
				var packedLen int
				for shift := uint(0); ; shift += 7 {
					if shift >= 64 {
						return ErrIntOverflowTest
					}
					if iNdEx >= l {
						return io.ErrUnexpectedEOF
					}
					b := dAtA[iNdEx]
					iNdEx++
					packedLen |= int(b&0x7F) << shift
					if b < 0x80 {
						break
					}
				}
				if packedLen < 0 {
					return ErrInvalidLengthTest
				}
				postIndex := iNdEx + packedLen
				if postIndex < 0 {
					return ErrInvalidLengthTest
				}
				if postIndex > l {
					return io.ErrUnexpectedEOF
				}
				var elementCount int
				var count int
				for _, integer := range dAtA[iNdEx:postIndex] {
					if integer < 128 {
						count++
					}
				}
				elementCount = count
				if elementCount != 0 && len(m.TimeTracer) == 0 {
					m.TimeTracer = make([]int64, 0, elementCount)
				}
				for iNdEx < postIndex {
					var v int64
					for shift := uint(0); ; shift += 7 {
						if shift >= 64 {
							return ErrIntOverflowTest
						}
						if iNdEx >= l {
							return io.ErrUnexpectedEOF
						}
						b := dAtA[iNdEx]
						iNdEx++
						v |= int64(b&0x7F) << shift
						if b < 0x80 {
							break
						}
					}
					m.TimeTracer = append(m.TimeTracer, v)
				}
			} else {
				return fmt.Errorf("proto: wrong wireType = %d for field TimeTracer", wireType)
			}
		case 7:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Err", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowTest
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if msglen < 0 {
				return ErrInvalidLengthTest
			}
			postIndex := iNdEx + msglen
			if postIndex < 0 {
				return ErrInvalidLengthTest
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			if m.Err == nil {
				m.Err = &Error{}
			}
			if err := m.Err.Unmarshal(dAtA[iNdEx:postIndex]); err != nil {
				return err
			}
			iNdEx = postIndex
		default:
			iNdEx = preIndex
			skippy, err := skipTest(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if (skippy < 0) || (iNdEx+skippy) < 0 {
				return ErrInvalidLengthTest
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			iNdEx += skippy
		}
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}
func (m *TestBinlogRequest) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowTest
			}
			if iNdEx >= l {
				return io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		if wireType == 4 {
			return fmt.Errorf("proto: TestBinlogRequest: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: TestBinlogRequest: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field Gtid", wireType)
			}
			m.Gtid = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowTest
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				m.Gtid |= int32(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
		default:
			iNdEx = preIndex
			skippy, err := skipTest(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if (skippy < 0) || (iNdEx+skippy) < 0 {
				return ErrInvalidLengthTest
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			iNdEx += skippy
		}
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}
func skipTest(dAtA []byte) (n int, err error) {
	l := len(dAtA)
	iNdEx := 0
	depth := 0
	for iNdEx < l {
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return 0, ErrIntOverflowTest
			}
			if iNdEx >= l {
				return 0, io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= (uint64(b) & 0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		wireType := int(wire & 0x7)
		switch wireType {
		case 0:
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return 0, ErrIntOverflowTest
				}
				if iNdEx >= l {
					return 0, io.ErrUnexpectedEOF
				}
				iNdEx++
				if dAtA[iNdEx-1] < 0x80 {
					break
				}
			}
		case 1:
			iNdEx += 8
		case 2:
			var length int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return 0, ErrIntOverflowTest
				}
				if iNdEx >= l {
					return 0, io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				length |= (int(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if length < 0 {
				return 0, ErrInvalidLengthTest
			}
			iNdEx += length
		case 3:
			depth++
		case 4:
			if depth == 0 {
				return 0, ErrUnexpectedEndOfGroupTest
			}
			depth--
		case 5:
			iNdEx += 4
		default:
			return 0, fmt.Errorf("proto: illegal wireType %d", wireType)
		}
		if iNdEx < 0 {
			return 0, ErrInvalidLengthTest
		}
		if depth == 0 {
			return iNdEx, nil
		}
	}
	return 0, io.ErrUnexpectedEOF
}

var (
	ErrInvalidLengthTest        = fmt.Errorf("proto: negative length found during unmarshaling")
	ErrIntOverflowTest          = fmt.Errorf("proto: integer overflow")
	ErrUnexpectedEndOfGroupTest = fmt.Errorf("proto: unexpected end of group")
)
