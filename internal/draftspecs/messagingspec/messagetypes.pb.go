// Code generated by protoc-gen-go. DO NOT EDIT.
// source: internal/draftspecs/messagingspec/messagetypes.proto

package messagingspec

import (
	fmt "fmt"
	proto "github.com/golang/protobuf/proto"
	math "math"
)

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

// This is a compile-time assertion to ensure that this generated file
// is compatible with the proto package it is being compiled against.
// A compilation error at this line likely means your copy of the
// proto package needs to be updated.
const _ = proto.ProtoPackageIsVersion3 // please upgrade the proto package

type MessageTypesRequest struct {
	// ApplicationKey is the identity key of the application to query.
	ApplicationKey       string   `protobuf:"bytes,1,opt,name=application_key,json=applicationKey,proto3" json:"application_key,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *MessageTypesRequest) Reset()         { *m = MessageTypesRequest{} }
func (m *MessageTypesRequest) String() string { return proto.CompactTextString(m) }
func (*MessageTypesRequest) ProtoMessage()    {}
func (*MessageTypesRequest) Descriptor() ([]byte, []int) {
	return fileDescriptor_319d39aab835adc5, []int{0}
}

func (m *MessageTypesRequest) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_MessageTypesRequest.Unmarshal(m, b)
}
func (m *MessageTypesRequest) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_MessageTypesRequest.Marshal(b, m, deterministic)
}
func (m *MessageTypesRequest) XXX_Merge(src proto.Message) {
	xxx_messageInfo_MessageTypesRequest.Merge(m, src)
}
func (m *MessageTypesRequest) XXX_Size() int {
	return xxx_messageInfo_MessageTypesRequest.Size(m)
}
func (m *MessageTypesRequest) XXX_DiscardUnknown() {
	xxx_messageInfo_MessageTypesRequest.DiscardUnknown(m)
}

var xxx_messageInfo_MessageTypesRequest proto.InternalMessageInfo

func (m *MessageTypesRequest) GetApplicationKey() string {
	if m != nil {
		return m.ApplicationKey
	}
	return ""
}

type MessageTypesResponse struct {
	// MessageTypes is the set of messages supported by the server.
	MessageTypes         []*MessageType `protobuf:"bytes,1,rep,name=message_types,json=messageTypes,proto3" json:"message_types,omitempty"`
	XXX_NoUnkeyedLiteral struct{}       `json:"-"`
	XXX_unrecognized     []byte         `json:"-"`
	XXX_sizecache        int32          `json:"-"`
}

func (m *MessageTypesResponse) Reset()         { *m = MessageTypesResponse{} }
func (m *MessageTypesResponse) String() string { return proto.CompactTextString(m) }
func (*MessageTypesResponse) ProtoMessage()    {}
func (*MessageTypesResponse) Descriptor() ([]byte, []int) {
	return fileDescriptor_319d39aab835adc5, []int{1}
}

func (m *MessageTypesResponse) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_MessageTypesResponse.Unmarshal(m, b)
}
func (m *MessageTypesResponse) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_MessageTypesResponse.Marshal(b, m, deterministic)
}
func (m *MessageTypesResponse) XXX_Merge(src proto.Message) {
	xxx_messageInfo_MessageTypesResponse.Merge(m, src)
}
func (m *MessageTypesResponse) XXX_Size() int {
	return xxx_messageInfo_MessageTypesResponse.Size(m)
}
func (m *MessageTypesResponse) XXX_DiscardUnknown() {
	xxx_messageInfo_MessageTypesResponse.DiscardUnknown(m)
}

var xxx_messageInfo_MessageTypesResponse proto.InternalMessageInfo

func (m *MessageTypesResponse) GetMessageTypes() []*MessageType {
	if m != nil {
		return m.MessageTypes
	}
	return nil
}

type MessageType struct {
	// PortableName is the unique name used to identity messages of this type.
	PortableName string `protobuf:"bytes,1,opt,name=portable_name,json=portableName,proto3" json:"portable_name,omitempty"`
	// ConfigName is the name used to identify this message type in the
	// dogma.config.v1 API.
	//
	// This name may differ across builds, as it is based on the fully-qualified
	// Go type name.
	ConfigName string `protobuf:"bytes,2,opt,name=config_name,json=configName,proto3" json:"config_name,omitempty"`
	// MediaTypes is a list of MIME media-types that the server may use to
	// represent messages of this type.
	MediaTypes           []string `protobuf:"bytes,3,rep,name=media_types,json=mediaTypes,proto3" json:"media_types,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *MessageType) Reset()         { *m = MessageType{} }
func (m *MessageType) String() string { return proto.CompactTextString(m) }
func (*MessageType) ProtoMessage()    {}
func (*MessageType) Descriptor() ([]byte, []int) {
	return fileDescriptor_319d39aab835adc5, []int{2}
}

func (m *MessageType) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_MessageType.Unmarshal(m, b)
}
func (m *MessageType) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_MessageType.Marshal(b, m, deterministic)
}
func (m *MessageType) XXX_Merge(src proto.Message) {
	xxx_messageInfo_MessageType.Merge(m, src)
}
func (m *MessageType) XXX_Size() int {
	return xxx_messageInfo_MessageType.Size(m)
}
func (m *MessageType) XXX_DiscardUnknown() {
	xxx_messageInfo_MessageType.DiscardUnknown(m)
}

var xxx_messageInfo_MessageType proto.InternalMessageInfo

func (m *MessageType) GetPortableName() string {
	if m != nil {
		return m.PortableName
	}
	return ""
}

func (m *MessageType) GetConfigName() string {
	if m != nil {
		return m.ConfigName
	}
	return ""
}

func (m *MessageType) GetMediaTypes() []string {
	if m != nil {
		return m.MediaTypes
	}
	return nil
}

func init() {
	proto.RegisterType((*MessageTypesRequest)(nil), "dogma.messaging.v1.MessageTypesRequest")
	proto.RegisterType((*MessageTypesResponse)(nil), "dogma.messaging.v1.MessageTypesResponse")
	proto.RegisterType((*MessageType)(nil), "dogma.messaging.v1.MessageType")
}

func init() {
	proto.RegisterFile("internal/draftspecs/messagingspec/messagetypes.proto", fileDescriptor_319d39aab835adc5)
}

var fileDescriptor_319d39aab835adc5 = []byte{
	// 266 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0x84, 0x90, 0xbd, 0x4b, 0xf4, 0x40,
	0x10, 0xc6, 0xc9, 0x1b, 0x78, 0xc1, 0xcd, 0x9d, 0xc2, 0x6a, 0x71, 0xdd, 0x85, 0x58, 0x78, 0xd5,
	0x06, 0x3f, 0x3a, 0xd1, 0x42, 0xec, 0x44, 0x8b, 0x60, 0x25, 0x42, 0xd8, 0x24, 0x93, 0x38, 0x98,
	0xfd, 0xb8, 0xec, 0x9c, 0x98, 0xff, 0x5e, 0xb2, 0xc9, 0x49, 0xc4, 0xc2, 0x72, 0x9e, 0xfd, 0xed,
	0xcc, 0x8f, 0x87, 0x5d, 0xa1, 0x26, 0xe8, 0xb4, 0x6c, 0xd3, 0xaa, 0x93, 0x35, 0x39, 0x0b, 0xa5,
	0x4b, 0x15, 0x38, 0x27, 0x1b, 0xd4, 0xcd, 0x30, 0x4e, 0x13, 0x50, 0x6f, 0xc1, 0x09, 0xdb, 0x19,
	0x32, 0x9c, 0x57, 0xa6, 0x51, 0x52, 0x7c, 0x73, 0xe2, 0xe3, 0x3c, 0xb9, 0x65, 0xc7, 0x8f, 0x23,
	0xf9, 0x3c, 0x90, 0x19, 0x6c, 0x77, 0xe0, 0x88, 0x9f, 0xb1, 0x23, 0x69, 0x6d, 0x8b, 0xa5, 0x24,
	0x34, 0x3a, 0x7f, 0x87, 0x7e, 0x15, 0xc4, 0xc1, 0xe6, 0x20, 0x3b, 0x9c, 0xc5, 0x0f, 0xd0, 0x27,
	0xaf, 0xec, 0xe4, 0xe7, 0x7f, 0x67, 0x8d, 0x76, 0xc0, 0xef, 0xd9, 0x72, 0x32, 0xc8, 0xbd, 0xc2,
	0x2a, 0x88, 0xc3, 0x4d, 0x74, 0xb1, 0x16, 0xbf, 0x1d, 0xc4, 0x6c, 0x41, 0xb6, 0x50, 0xb3, 0x6d,
	0x09, 0xb1, 0x68, 0xf6, 0xc8, 0x4f, 0xd9, 0xd2, 0x9a, 0x8e, 0x64, 0xd1, 0x42, 0xae, 0xa5, 0x82,
	0xc9, 0x69, 0xb1, 0x0f, 0x9f, 0xa4, 0x02, 0xbe, 0x66, 0x51, 0x69, 0x74, 0x8d, 0xcd, 0x88, 0xfc,
	0xf3, 0x08, 0x1b, 0xa3, 0x3d, 0xa0, 0xa0, 0x42, 0x39, 0x89, 0x85, 0x71, 0x38, 0x00, 0x3e, 0xf2,
	0x57, 0xef, 0x6e, 0x5e, 0xae, 0x1b, 0xa4, 0xb7, 0x5d, 0x21, 0x4a, 0xa3, 0x52, 0x2f, 0x4c, 0xb8,
	0x4d, 0x51, 0xd7, 0xf8, 0x99, 0xfe, 0xd9, 0x7c, 0xf1, 0xdf, 0xb7, 0x7d, 0xf9, 0x15, 0x00, 0x00,
	0xff, 0xff, 0xe5, 0x75, 0x3b, 0x6c, 0xa5, 0x01, 0x00, 0x00,
}
