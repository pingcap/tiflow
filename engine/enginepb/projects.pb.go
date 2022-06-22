// Code generated by protoc-gen-gogo. DO NOT EDIT.
// source: projects.proto

package enginepb

import (
	fmt "fmt"
	io "io"
	math "math"
	math_bits "math/bits"

	proto "github.com/gogo/protobuf/proto"
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

type ProjectInfo struct {
	TenantId  string `protobuf:"bytes,1,opt,name=tenant_id,json=tenantId,proto3" json:"tenant_id,omitempty"`
	ProjectId string `protobuf:"bytes,2,opt,name=project_id,json=projectId,proto3" json:"project_id,omitempty"`
}

func (m *ProjectInfo) Reset()         { *m = ProjectInfo{} }
func (m *ProjectInfo) String() string { return proto.CompactTextString(m) }
func (*ProjectInfo) ProtoMessage()    {}
func (*ProjectInfo) Descriptor() ([]byte, []int) {
	return fileDescriptor_a2826043bb82544a, []int{0}
}
func (m *ProjectInfo) XXX_Unmarshal(b []byte) error {
	return m.Unmarshal(b)
}
func (m *ProjectInfo) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	if deterministic {
		return xxx_messageInfo_ProjectInfo.Marshal(b, m, deterministic)
	} else {
		b = b[:cap(b)]
		n, err := m.MarshalToSizedBuffer(b)
		if err != nil {
			return nil, err
		}
		return b[:n], nil
	}
}
func (m *ProjectInfo) XXX_Merge(src proto.Message) {
	xxx_messageInfo_ProjectInfo.Merge(m, src)
}
func (m *ProjectInfo) XXX_Size() int {
	return m.Size()
}
func (m *ProjectInfo) XXX_DiscardUnknown() {
	xxx_messageInfo_ProjectInfo.DiscardUnknown(m)
}

var xxx_messageInfo_ProjectInfo proto.InternalMessageInfo

func (m *ProjectInfo) GetTenantId() string {
	if m != nil {
		return m.TenantId
	}
	return ""
}

func (m *ProjectInfo) GetProjectId() string {
	if m != nil {
		return m.ProjectId
	}
	return ""
}

func init() {
	proto.RegisterType((*ProjectInfo)(nil), "enginepb.ProjectInfo")
}

func init() { proto.RegisterFile("projects.proto", fileDescriptor_a2826043bb82544a) }

var fileDescriptor_a2826043bb82544a = []byte{
	// 134 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0xe2, 0xe2, 0x2b, 0x28, 0xca, 0xcf,
	0x4a, 0x4d, 0x2e, 0x29, 0xd6, 0x2b, 0x28, 0xca, 0x2f, 0xc9, 0x17, 0xe2, 0x48, 0xcd, 0x4b, 0xcf,
	0xcc, 0x4b, 0x2d, 0x48, 0x52, 0xf2, 0xe4, 0xe2, 0x0e, 0x80, 0xc8, 0x79, 0xe6, 0xa5, 0xe5, 0x0b,
	0x49, 0x73, 0x71, 0x96, 0xa4, 0xe6, 0x25, 0xe6, 0x95, 0xc4, 0x67, 0xa6, 0x48, 0x30, 0x2a, 0x30,
	0x6a, 0x70, 0x06, 0x71, 0x40, 0x04, 0x3c, 0x53, 0x84, 0x64, 0xb9, 0xb8, 0xa0, 0xe6, 0x80, 0x64,
	0x99, 0xc0, 0xb2, 0x9c, 0x50, 0x11, 0xcf, 0x14, 0x27, 0x89, 0x13, 0x8f, 0xe4, 0x18, 0x2f, 0x3c,
	0x92, 0x63, 0x7c, 0xf0, 0x48, 0x8e, 0x71, 0xc2, 0x63, 0x39, 0x86, 0x0b, 0x8f, 0xe5, 0x18, 0x6e,
	0x3c, 0x96, 0x63, 0x48, 0x62, 0x03, 0xdb, 0x6a, 0x0c, 0x08, 0x00, 0x00, 0xff, 0xff, 0xf7, 0xee,
	0x13, 0xde, 0x87, 0x00, 0x00, 0x00,
}

func (m *ProjectInfo) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalToSizedBuffer(dAtA[:size])
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *ProjectInfo) MarshalTo(dAtA []byte) (int, error) {
	size := m.Size()
	return m.MarshalToSizedBuffer(dAtA[:size])
}

func (m *ProjectInfo) MarshalToSizedBuffer(dAtA []byte) (int, error) {
	i := len(dAtA)
	_ = i
	var l int
	_ = l
	if len(m.ProjectId) > 0 {
		i -= len(m.ProjectId)
		copy(dAtA[i:], m.ProjectId)
		i = encodeVarintProjects(dAtA, i, uint64(len(m.ProjectId)))
		i--
		dAtA[i] = 0x12
	}
	if len(m.TenantId) > 0 {
		i -= len(m.TenantId)
		copy(dAtA[i:], m.TenantId)
		i = encodeVarintProjects(dAtA, i, uint64(len(m.TenantId)))
		i--
		dAtA[i] = 0xa
	}
	return len(dAtA) - i, nil
}

func encodeVarintProjects(dAtA []byte, offset int, v uint64) int {
	offset -= sovProjects(v)
	base := offset
	for v >= 1<<7 {
		dAtA[offset] = uint8(v&0x7f | 0x80)
		v >>= 7
		offset++
	}
	dAtA[offset] = uint8(v)
	return base
}
func (m *ProjectInfo) Size() (n int) {
	if m == nil {
		return 0
	}
	var l int
	_ = l
	l = len(m.TenantId)
	if l > 0 {
		n += 1 + l + sovProjects(uint64(l))
	}
	l = len(m.ProjectId)
	if l > 0 {
		n += 1 + l + sovProjects(uint64(l))
	}
	return n
}

func sovProjects(x uint64) (n int) {
	return (math_bits.Len64(x|1) + 6) / 7
}
func sozProjects(x uint64) (n int) {
	return sovProjects(uint64((x << 1) ^ uint64((int64(x) >> 63))))
}
func (m *ProjectInfo) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowProjects
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
			return fmt.Errorf("proto: ProjectInfo: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: ProjectInfo: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field TenantId", wireType)
			}
			var stringLen uint64
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowProjects
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				stringLen |= uint64(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			intStringLen := int(stringLen)
			if intStringLen < 0 {
				return ErrInvalidLengthProjects
			}
			postIndex := iNdEx + intStringLen
			if postIndex < 0 {
				return ErrInvalidLengthProjects
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.TenantId = string(dAtA[iNdEx:postIndex])
			iNdEx = postIndex
		case 2:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field ProjectId", wireType)
			}
			var stringLen uint64
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowProjects
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				stringLen |= uint64(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			intStringLen := int(stringLen)
			if intStringLen < 0 {
				return ErrInvalidLengthProjects
			}
			postIndex := iNdEx + intStringLen
			if postIndex < 0 {
				return ErrInvalidLengthProjects
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.ProjectId = string(dAtA[iNdEx:postIndex])
			iNdEx = postIndex
		default:
			iNdEx = preIndex
			skippy, err := skipProjects(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if (skippy < 0) || (iNdEx+skippy) < 0 {
				return ErrInvalidLengthProjects
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
func skipProjects(dAtA []byte) (n int, err error) {
	l := len(dAtA)
	iNdEx := 0
	depth := 0
	for iNdEx < l {
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return 0, ErrIntOverflowProjects
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
					return 0, ErrIntOverflowProjects
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
					return 0, ErrIntOverflowProjects
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
				return 0, ErrInvalidLengthProjects
			}
			iNdEx += length
		case 3:
			depth++
		case 4:
			if depth == 0 {
				return 0, ErrUnexpectedEndOfGroupProjects
			}
			depth--
		case 5:
			iNdEx += 4
		default:
			return 0, fmt.Errorf("proto: illegal wireType %d", wireType)
		}
		if iNdEx < 0 {
			return 0, ErrInvalidLengthProjects
		}
		if depth == 0 {
			return iNdEx, nil
		}
	}
	return 0, io.ErrUnexpectedEOF
}

var (
	ErrInvalidLengthProjects        = fmt.Errorf("proto: negative length found during unmarshaling")
	ErrIntOverflowProjects          = fmt.Errorf("proto: integer overflow")
	ErrUnexpectedEndOfGroupProjects = fmt.Errorf("proto: unexpected end of group")
)
