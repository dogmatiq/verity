// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.36.6
// 	protoc        v6.31.1
// source: github.com/dogmatiq/verity/persistence/boltpersistence/internal/pb/aggregate.proto

package pb

import (
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	reflect "reflect"
	sync "sync"
	unsafe "unsafe"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

// AggregateMetaData is a protocol buffers representation of
// persistence.AggregateMetaData.
type AggregateMetaData struct {
	state          protoimpl.MessageState `protogen:"open.v1"`
	Revision       uint64                 `protobuf:"varint,1,opt,name=revision,proto3" json:"revision,omitempty"`
	InstanceExists bool                   `protobuf:"varint,2,opt,name=instance_exists,json=instanceExists,proto3" json:"instance_exists,omitempty"`
	LastEventId    string                 `protobuf:"bytes,3,opt,name=last_event_id,json=lastEventId,proto3" json:"last_event_id,omitempty"`
	BarrierEventId string                 `protobuf:"bytes,4,opt,name=barrier_event_id,json=barrierEventId,proto3" json:"barrier_event_id,omitempty"`
	unknownFields  protoimpl.UnknownFields
	sizeCache      protoimpl.SizeCache
}

func (x *AggregateMetaData) Reset() {
	*x = AggregateMetaData{}
	mi := &file_github_com_dogmatiq_verity_persistence_boltpersistence_internal_pb_aggregate_proto_msgTypes[0]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *AggregateMetaData) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*AggregateMetaData) ProtoMessage() {}

func (x *AggregateMetaData) ProtoReflect() protoreflect.Message {
	mi := &file_github_com_dogmatiq_verity_persistence_boltpersistence_internal_pb_aggregate_proto_msgTypes[0]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use AggregateMetaData.ProtoReflect.Descriptor instead.
func (*AggregateMetaData) Descriptor() ([]byte, []int) {
	return file_github_com_dogmatiq_verity_persistence_boltpersistence_internal_pb_aggregate_proto_rawDescGZIP(), []int{0}
}

func (x *AggregateMetaData) GetRevision() uint64 {
	if x != nil {
		return x.Revision
	}
	return 0
}

func (x *AggregateMetaData) GetInstanceExists() bool {
	if x != nil {
		return x.InstanceExists
	}
	return false
}

func (x *AggregateMetaData) GetLastEventId() string {
	if x != nil {
		return x.LastEventId
	}
	return ""
}

func (x *AggregateMetaData) GetBarrierEventId() string {
	if x != nil {
		return x.BarrierEventId
	}
	return ""
}

var File_github_com_dogmatiq_verity_persistence_boltpersistence_internal_pb_aggregate_proto protoreflect.FileDescriptor

const file_github_com_dogmatiq_verity_persistence_boltpersistence_internal_pb_aggregate_proto_rawDesc = "" +
	"\n" +
	"Rgithub.com/dogmatiq/verity/persistence/boltpersistence/internal/pb/aggregate.proto\x12\x1cverity.persistence.boltdb.v1\"\xa6\x01\n" +
	"\x11AggregateMetaData\x12\x1a\n" +
	"\brevision\x18\x01 \x01(\x04R\brevision\x12'\n" +
	"\x0finstance_exists\x18\x02 \x01(\bR\x0einstanceExists\x12\"\n" +
	"\rlast_event_id\x18\x03 \x01(\tR\vlastEventId\x12(\n" +
	"\x10barrier_event_id\x18\x04 \x01(\tR\x0ebarrierEventIdBDZBgithub.com/dogmatiq/verity/persistence/boltpersistence/internal/pbb\x06proto3"

var (
	file_github_com_dogmatiq_verity_persistence_boltpersistence_internal_pb_aggregate_proto_rawDescOnce sync.Once
	file_github_com_dogmatiq_verity_persistence_boltpersistence_internal_pb_aggregate_proto_rawDescData []byte
)

func file_github_com_dogmatiq_verity_persistence_boltpersistence_internal_pb_aggregate_proto_rawDescGZIP() []byte {
	file_github_com_dogmatiq_verity_persistence_boltpersistence_internal_pb_aggregate_proto_rawDescOnce.Do(func() {
		file_github_com_dogmatiq_verity_persistence_boltpersistence_internal_pb_aggregate_proto_rawDescData = protoimpl.X.CompressGZIP(unsafe.Slice(unsafe.StringData(file_github_com_dogmatiq_verity_persistence_boltpersistence_internal_pb_aggregate_proto_rawDesc), len(file_github_com_dogmatiq_verity_persistence_boltpersistence_internal_pb_aggregate_proto_rawDesc)))
	})
	return file_github_com_dogmatiq_verity_persistence_boltpersistence_internal_pb_aggregate_proto_rawDescData
}

var file_github_com_dogmatiq_verity_persistence_boltpersistence_internal_pb_aggregate_proto_msgTypes = make([]protoimpl.MessageInfo, 1)
var file_github_com_dogmatiq_verity_persistence_boltpersistence_internal_pb_aggregate_proto_goTypes = []any{
	(*AggregateMetaData)(nil), // 0: verity.persistence.boltdb.v1.AggregateMetaData
}
var file_github_com_dogmatiq_verity_persistence_boltpersistence_internal_pb_aggregate_proto_depIdxs = []int32{
	0, // [0:0] is the sub-list for method output_type
	0, // [0:0] is the sub-list for method input_type
	0, // [0:0] is the sub-list for extension type_name
	0, // [0:0] is the sub-list for extension extendee
	0, // [0:0] is the sub-list for field type_name
}

func init() {
	file_github_com_dogmatiq_verity_persistence_boltpersistence_internal_pb_aggregate_proto_init()
}
func file_github_com_dogmatiq_verity_persistence_boltpersistence_internal_pb_aggregate_proto_init() {
	if File_github_com_dogmatiq_verity_persistence_boltpersistence_internal_pb_aggregate_proto != nil {
		return
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: unsafe.Slice(unsafe.StringData(file_github_com_dogmatiq_verity_persistence_boltpersistence_internal_pb_aggregate_proto_rawDesc), len(file_github_com_dogmatiq_verity_persistence_boltpersistence_internal_pb_aggregate_proto_rawDesc)),
			NumEnums:      0,
			NumMessages:   1,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_github_com_dogmatiq_verity_persistence_boltpersistence_internal_pb_aggregate_proto_goTypes,
		DependencyIndexes: file_github_com_dogmatiq_verity_persistence_boltpersistence_internal_pb_aggregate_proto_depIdxs,
		MessageInfos:      file_github_com_dogmatiq_verity_persistence_boltpersistence_internal_pb_aggregate_proto_msgTypes,
	}.Build()
	File_github_com_dogmatiq_verity_persistence_boltpersistence_internal_pb_aggregate_proto = out.File
	file_github_com_dogmatiq_verity_persistence_boltpersistence_internal_pb_aggregate_proto_goTypes = nil
	file_github_com_dogmatiq_verity_persistence_boltpersistence_internal_pb_aggregate_proto_depIdxs = nil
}
