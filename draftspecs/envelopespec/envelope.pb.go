// Code generated by protoc-gen-go. DO NOT EDIT.
// source: draftspecs/envelopespec/envelope.proto

package envelopespec

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

// Identity is a protocol buffers representation of the configkit.Identity type.
type Identity struct {
	// Name is the entity's unique name.
	Name string `protobuf:"bytes,1,opt,name=name,proto3" json:"name,omitempty"`
	// Key is the entity's immutable, unique key.
	Key                  string   `protobuf:"bytes,2,opt,name=key,proto3" json:"key,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *Identity) Reset()         { *m = Identity{} }
func (m *Identity) String() string { return proto.CompactTextString(m) }
func (*Identity) ProtoMessage()    {}
func (*Identity) Descriptor() ([]byte, []int) {
	return fileDescriptor_291e8cfade150cd6, []int{0}
}

func (m *Identity) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_Identity.Unmarshal(m, b)
}
func (m *Identity) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_Identity.Marshal(b, m, deterministic)
}
func (m *Identity) XXX_Merge(src proto.Message) {
	xxx_messageInfo_Identity.Merge(m, src)
}
func (m *Identity) XXX_Size() int {
	return xxx_messageInfo_Identity.Size(m)
}
func (m *Identity) XXX_DiscardUnknown() {
	xxx_messageInfo_Identity.DiscardUnknown(m)
}

var xxx_messageInfo_Identity proto.InternalMessageInfo

func (m *Identity) GetName() string {
	if m != nil {
		return m.Name
	}
	return ""
}

func (m *Identity) GetKey() string {
	if m != nil {
		return m.Key
	}
	return ""
}

type Source struct {
	// Application is the identity of the Dogma application that produced this
	// message.
	Application *Identity `protobuf:"bytes,1,opt,name=application,proto3" json:"application,omitempty"`
	// Handler is the identity of the handler that produced the message. It is the
	// zero-value if the message was not produced by a handler.
	Handler *Identity `protobuf:"bytes,2,opt,name=handler,proto3" json:"handler,omitempty"`
	// InstanceID is the aggregate or process instance that produced the message.
	// It is empty if the message was not produced by a handler, or it was
	// produced by an integration handler.
	InstanceId           string   `protobuf:"bytes,3,opt,name=instance_id,json=instanceId,proto3" json:"instance_id,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *Source) Reset()         { *m = Source{} }
func (m *Source) String() string { return proto.CompactTextString(m) }
func (*Source) ProtoMessage()    {}
func (*Source) Descriptor() ([]byte, []int) {
	return fileDescriptor_291e8cfade150cd6, []int{1}
}

func (m *Source) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_Source.Unmarshal(m, b)
}
func (m *Source) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_Source.Marshal(b, m, deterministic)
}
func (m *Source) XXX_Merge(src proto.Message) {
	xxx_messageInfo_Source.Merge(m, src)
}
func (m *Source) XXX_Size() int {
	return xxx_messageInfo_Source.Size(m)
}
func (m *Source) XXX_DiscardUnknown() {
	xxx_messageInfo_Source.DiscardUnknown(m)
}

var xxx_messageInfo_Source proto.InternalMessageInfo

func (m *Source) GetApplication() *Identity {
	if m != nil {
		return m.Application
	}
	return nil
}

func (m *Source) GetHandler() *Identity {
	if m != nil {
		return m.Handler
	}
	return nil
}

func (m *Source) GetInstanceId() string {
	if m != nil {
		return m.InstanceId
	}
	return ""
}

// MetaData is a protocol buffers representation of the envelope.MetaData type.
type MetaData struct {
	// MessageID is a unique identifier for the message.
	MessageId string `protobuf:"bytes,1,opt,name=message_id,json=messageId,proto3" json:"message_id,omitempty"`
	// CausationID is the ID of the message that was being handled when the
	// message identified by MessageID was produced.
	CausationId string `protobuf:"bytes,2,opt,name=causation_id,json=causationId,proto3" json:"causation_id,omitempty"`
	// CorrelationID is the ID of the "root" message that entered the application
	// to cause the message identified by MessageID, either directly or
	// indirectly.
	CorrelationId string `protobuf:"bytes,3,opt,name=correlation_id,json=correlationId,proto3" json:"correlation_id,omitempty"`
	// Source describes the source of the message.
	Source *Source `protobuf:"bytes,4,opt,name=source,proto3" json:"source,omitempty"`
	// CreatedAt is the time at which the message was created, marshaled in
	// binary format.
	CreatedAt []byte `protobuf:"bytes,5,opt,name=created_at,json=createdAt,proto3" json:"created_at,omitempty"`
	// ScheduledFor is the time at which a timeout message was scheduled,
	// marshaled in binary format.
	ScheduledFor         []byte   `protobuf:"bytes,6,opt,name=scheduled_for,json=scheduledFor,proto3" json:"scheduled_for,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *MetaData) Reset()         { *m = MetaData{} }
func (m *MetaData) String() string { return proto.CompactTextString(m) }
func (*MetaData) ProtoMessage()    {}
func (*MetaData) Descriptor() ([]byte, []int) {
	return fileDescriptor_291e8cfade150cd6, []int{2}
}

func (m *MetaData) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_MetaData.Unmarshal(m, b)
}
func (m *MetaData) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_MetaData.Marshal(b, m, deterministic)
}
func (m *MetaData) XXX_Merge(src proto.Message) {
	xxx_messageInfo_MetaData.Merge(m, src)
}
func (m *MetaData) XXX_Size() int {
	return xxx_messageInfo_MetaData.Size(m)
}
func (m *MetaData) XXX_DiscardUnknown() {
	xxx_messageInfo_MetaData.DiscardUnknown(m)
}

var xxx_messageInfo_MetaData proto.InternalMessageInfo

func (m *MetaData) GetMessageId() string {
	if m != nil {
		return m.MessageId
	}
	return ""
}

func (m *MetaData) GetCausationId() string {
	if m != nil {
		return m.CausationId
	}
	return ""
}

func (m *MetaData) GetCorrelationId() string {
	if m != nil {
		return m.CorrelationId
	}
	return ""
}

func (m *MetaData) GetSource() *Source {
	if m != nil {
		return m.Source
	}
	return nil
}

func (m *MetaData) GetCreatedAt() []byte {
	if m != nil {
		return m.CreatedAt
	}
	return nil
}

func (m *MetaData) GetScheduledFor() []byte {
	if m != nil {
		return m.ScheduledFor
	}
	return nil
}

// Envelope is a protocol buffers representation of the envelope.Envelope type.
type Envelope struct {
	// MetaData is the message meta-data.
	MetaData *MetaData `protobuf:"bytes,1,opt,name=meta_data,json=metaData,proto3" json:"meta_data,omitempty"`
	// MediaType is a MIME media-type describing the content and encoding of the
	// binary message data.
	MediaType string `protobuf:"bytes,2,opt,name=media_type,json=mediaType,proto3" json:"media_type,omitempty"`
	// Data is the binary message data.
	Data                 []byte   `protobuf:"bytes,3,opt,name=data,proto3" json:"data,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *Envelope) Reset()         { *m = Envelope{} }
func (m *Envelope) String() string { return proto.CompactTextString(m) }
func (*Envelope) ProtoMessage()    {}
func (*Envelope) Descriptor() ([]byte, []int) {
	return fileDescriptor_291e8cfade150cd6, []int{3}
}

func (m *Envelope) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_Envelope.Unmarshal(m, b)
}
func (m *Envelope) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_Envelope.Marshal(b, m, deterministic)
}
func (m *Envelope) XXX_Merge(src proto.Message) {
	xxx_messageInfo_Envelope.Merge(m, src)
}
func (m *Envelope) XXX_Size() int {
	return xxx_messageInfo_Envelope.Size(m)
}
func (m *Envelope) XXX_DiscardUnknown() {
	xxx_messageInfo_Envelope.DiscardUnknown(m)
}

var xxx_messageInfo_Envelope proto.InternalMessageInfo

func (m *Envelope) GetMetaData() *MetaData {
	if m != nil {
		return m.MetaData
	}
	return nil
}

func (m *Envelope) GetMediaType() string {
	if m != nil {
		return m.MediaType
	}
	return ""
}

func (m *Envelope) GetData() []byte {
	if m != nil {
		return m.Data
	}
	return nil
}

func init() {
	proto.RegisterType((*Identity)(nil), "dogma.envelope.v1.Identity")
	proto.RegisterType((*Source)(nil), "dogma.envelope.v1.Source")
	proto.RegisterType((*MetaData)(nil), "dogma.envelope.v1.MetaData")
	proto.RegisterType((*Envelope)(nil), "dogma.envelope.v1.Envelope")
}

func init() {
	proto.RegisterFile("draftspecs/envelopespec/envelope.proto", fileDescriptor_291e8cfade150cd6)
}

var fileDescriptor_291e8cfade150cd6 = []byte{
	// 392 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0x84, 0x92, 0xcf, 0x8e, 0xd3, 0x30,
	0x10, 0xc6, 0x15, 0xba, 0x94, 0x66, 0xda, 0x45, 0xe0, 0x53, 0x10, 0x42, 0x2c, 0x45, 0xa0, 0x3d,
	0xa5, 0x94, 0x15, 0x12, 0x17, 0x0e, 0x20, 0x40, 0xca, 0x81, 0x4b, 0xe0, 0xc4, 0x25, 0x9a, 0xb5,
	0xa7, 0xad, 0x45, 0x62, 0x07, 0x7b, 0xba, 0xd0, 0xa7, 0xe1, 0xfd, 0x78, 0x0a, 0x94, 0x69, 0x1a,
	0x15, 0xf1, 0x67, 0x6f, 0xe3, 0x9f, 0x3e, 0xfb, 0x9b, 0xf9, 0x3c, 0xf0, 0xd4, 0x04, 0x5c, 0x71,
	0x6c, 0x49, 0xc7, 0x05, 0xb9, 0x2b, 0xaa, 0x7d, 0x4b, 0xdd, 0x69, 0x38, 0xe4, 0x6d, 0xf0, 0xec,
	0xd5, 0x5d, 0xe3, 0xd7, 0x0d, 0xe6, 0x03, 0xbd, 0x5a, 0xce, 0x9f, 0xc1, 0xa4, 0x30, 0xe4, 0xd8,
	0xf2, 0x4e, 0x29, 0x38, 0x71, 0xd8, 0x50, 0x96, 0x9c, 0x25, 0xe7, 0x69, 0x29, 0xb5, 0xba, 0x03,
	0xa3, 0x2f, 0xb4, 0xcb, 0x6e, 0x08, 0xea, 0xca, 0xf9, 0x8f, 0x04, 0xc6, 0x1f, 0xfd, 0x36, 0x68,
	0x52, 0xaf, 0x60, 0x8a, 0x6d, 0x5b, 0x5b, 0x8d, 0x6c, 0xbd, 0x93, 0x7b, 0xd3, 0xe7, 0xf7, 0xf3,
	0x3f, 0x5c, 0xf2, 0x83, 0x45, 0x79, 0xac, 0x57, 0x2f, 0xe0, 0xd6, 0x06, 0x9d, 0xa9, 0x29, 0xc8,
	0xfb, 0xd7, 0x5c, 0x3d, 0x68, 0xd5, 0x43, 0x98, 0x5a, 0x17, 0x19, 0x9d, 0xa6, 0xca, 0x9a, 0x6c,
	0x24, 0xad, 0xc1, 0x01, 0x15, 0x66, 0xfe, 0x33, 0x81, 0xc9, 0x07, 0x62, 0x7c, 0x8b, 0x8c, 0xea,
	0x01, 0x40, 0x43, 0x31, 0xe2, 0x5a, 0xc4, 0xfb, 0xd1, 0xd2, 0x9e, 0x14, 0x46, 0x3d, 0x82, 0x99,
	0xc6, 0x6d, 0x94, 0x86, 0x3a, 0xc1, 0x7e, 0xd0, 0xe9, 0xc0, 0x0a, 0xa3, 0x9e, 0xc0, 0x6d, 0xed,
	0x43, 0xa0, 0x7a, 0x10, 0xed, 0x2d, 0x4f, 0x8f, 0x68, 0x61, 0xd4, 0x12, 0xc6, 0x51, 0x62, 0xc9,
	0x4e, 0x64, 0x98, 0x7b, 0x7f, 0x19, 0x66, 0x9f, 0x5b, 0xd9, 0x0b, 0xbb, 0xde, 0x74, 0x20, 0x64,
	0x32, 0x15, 0x72, 0x76, 0xf3, 0x2c, 0x39, 0x9f, 0x95, 0x69, 0x4f, 0x5e, 0xb3, 0x7a, 0x0c, 0xa7,
	0x51, 0x6f, 0xc8, 0x6c, 0x6b, 0x32, 0xd5, 0xca, 0x87, 0x6c, 0x2c, 0x8a, 0xd9, 0x00, 0xdf, 0xfb,
	0x30, 0xff, 0x06, 0x93, 0x77, 0xbd, 0x83, 0x7a, 0x09, 0x69, 0x43, 0x8c, 0x95, 0x41, 0xc6, 0xff,
	0xfc, 0xc6, 0x21, 0x9b, 0x72, 0xd2, 0xfc, 0x96, 0x92, 0xb1, 0x58, 0xf1, 0xae, 0xa5, 0x3e, 0x84,
	0x54, 0xc8, 0xa7, 0x5d, 0x4b, 0xdd, 0x66, 0xc8, 0x9b, 0x23, 0x69, 0x40, 0xea, 0x37, 0x17, 0x9f,
	0x97, 0x6b, 0xcb, 0x9b, 0xed, 0x65, 0xae, 0x7d, 0xb3, 0x10, 0x17, 0xb6, 0x5f, 0x17, 0xd6, 0xad,
	0xec, 0xf7, 0xc5, 0x3f, 0x16, 0xf2, 0x72, 0x2c, 0x8b, 0x78, 0xf1, 0x2b, 0x00, 0x00, 0xff, 0xff,
	0x82, 0x86, 0x03, 0x94, 0xb2, 0x02, 0x00, 0x00,
}