package main

import (
	"context"
	"encoding/json"
	"fmt"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/runtime/protoimpl"
	"log"
	"net"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type BizManager struct{}

func (b *BizManager) Check(ctx context.Context, nothing *Nothing) (*Nothing, error) {
	return &Nothing{Dummy: true}, nil
}

func (b *BizManager) Add(ctx context.Context, nothing *Nothing) (*Nothing, error) {
	return &Nothing{Dummy: true}, nil
}

func (b *BizManager) Test(ctx context.Context, nothing *Nothing) (*Nothing, error) {
	return &Nothing{Dummy: true}, nil
}

type AdminManager struct {
	publisher *MethodCallPublisher
}

func (a *AdminManager) Logging(nothing *Nothing, stream Admin_LoggingServer) error {
	eventCh, subscribeID := a.publisher.Subscribe()
	defer a.publisher.Unsubscribe(subscribeID)

	for {
		select {
		case event := <-eventCh:
			if err := stream.Send(&Event{
				Timestamp: event.Timestamp,
				Consumer:  event.Consumer,
				Method:    event.Method,
				Host:      event.Host,
			}); err != nil {
				return status.Error(codes.Internal, "failed to send event")
			}
		case <-stream.Context().Done():
			return nil
		}
	}
}

func (a *AdminManager) Statistics(interval *StatInterval, stream Admin_StatisticsServer) error {
	eventCh, subscribeID := a.publisher.Subscribe()
	defer a.publisher.Unsubscribe(subscribeID)

	stat := new(Stat)
	ticker := time.NewTicker(time.Duration(interval.IntervalSeconds) * time.Second)

	for {
		if stat.ByMethod == nil {
			stat.ByMethod = make(map[string]uint64)
		}
		if stat.ByConsumer == nil {
			stat.ByConsumer = make(map[string]uint64)
		}

		select {
		case event := <-eventCh:
			stat.ByMethod[event.Method]++
			stat.ByConsumer[event.Consumer]++
		case <-ticker.C:
			stat.Timestamp = time.Now().Unix()
			if err := stream.Send(stat); err != nil {
				return status.Error(codes.Internal, "failed to send statistics")
			}

			stat.Reset()
		case <-stream.Context().Done():
			ticker.Stop()
			return nil
		}
	}
}

type ACLManager struct {
	mu  sync.RWMutex
	acl map[string][]string
}

func (m *ACLManager) Load(aclPayload []byte) error {
	var acl map[string][]string
	if err := json.Unmarshal(aclPayload, &acl); err != nil {
		return fmt.Errorf("failed to unmarshal acl payload: %v", err)
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	m.acl = acl

	return nil
}

func (m *ACLManager) HasAccess(key, val string) bool {
	if m.acl == nil {
		log.Println("ACLManager has empty access control list. Call Load() to fix it")
		return false
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	if _, exist := m.acl[key]; !exist {
		return false
	}

	for _, aclVal := range m.acl[key] {
		lastLetter := aclVal[len(aclVal)-1]
		if lastLetter == '*' {
			prefix := aclVal[:len(aclVal)-1]
			if strings.HasPrefix(val, prefix) {
				return true
			}

			continue
		}

		if aclVal == val {
			return true
		}
	}
	return false
}

type MethodCallEvent struct {
	Timestamp int64
	Consumer  string
	Method    string
	Host      string
}

type MethodCallPublisher struct {
	mu          sync.RWMutex
	subscribers map[int]chan MethodCallEvent

	idCounter int64
}

func NewMethodCallPublisher() *MethodCallPublisher {
	return &MethodCallPublisher{
		subscribers: make(map[int]chan MethodCallEvent),
	}
}

func (p *MethodCallPublisher) Notify(event MethodCallEvent) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	event.Timestamp = time.Now().Unix()
	for _, ch := range p.subscribers {
		ch <- event
	}
}

func (p *MethodCallPublisher) Subscribe() (ch <-chan MethodCallEvent, subscribeID int) {
	subscriberID := int(atomic.AddInt64(&p.idCounter, 1))

	p.mu.Lock()
	defer p.mu.Unlock()
	p.subscribers[subscriberID] = make(chan MethodCallEvent, 1)

	return p.subscribers[subscriberID], subscriberID
}

func (p *MethodCallPublisher) Unsubscribe(subscribeID int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.subscribers, subscribeID)
}

type ACLInterceptor struct {
	consumerKey string
	manager     *ACLManager
}

func (i *ACLInterceptor) Unary() grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req interface{},
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (resp interface{}, err error) {
		err = i.do(ctx, info.FullMethod)
		if err != nil {
			return nil, err
		}
		return handler(ctx, req)
	}
}

func (i *ACLInterceptor) Stream() grpc.StreamServerInterceptor {
	return func(
		srv interface{},
		stream grpc.ServerStream,
		info *grpc.StreamServerInfo,
		handler grpc.StreamHandler,
	) error {
		err := i.do(stream.Context(), info.FullMethod)
		if err != nil {
			return err
		}
		return handler(srv, stream)
	}
}

func (i *ACLInterceptor) do(ctx context.Context, method string) error {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return status.Error(
			codes.Unauthenticated,
			"consumer doesn't have permissions to call this method",
		)
	}

	for _, k := range md.Get(i.consumerKey) {
		if i.manager.HasAccess(k, method) {
			return nil
		}
	}

	return status.Error(
		codes.Unauthenticated,
		"consumer doesn't have permissions to call this method",
	)
}

type MethodCallNotifyInterceptor struct {
	consumerKey string
	publisher   *MethodCallPublisher
}

func (i *MethodCallNotifyInterceptor) Unary() grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req interface{},
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (resp interface{}, err error) {
		if err = i.do(ctx, info.FullMethod); err != nil {
			return nil, err
		}
		return handler(ctx, req)
	}
}

func (i *MethodCallNotifyInterceptor) Stream() grpc.StreamServerInterceptor {
	return func(
		srv interface{},
		stream grpc.ServerStream,
		info *grpc.StreamServerInfo,
		handler grpc.StreamHandler,
	) error {
		if err := i.do(stream.Context(), info.FullMethod); err != nil {
			return err
		}
		return handler(srv, stream)
	}
}

func (i *MethodCallNotifyInterceptor) do(ctx context.Context, method string) error {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return nil
	}

	pr, ok := peer.FromContext(ctx)
	if !ok {
		return nil
	}

	consumers := md.Get(i.consumerKey)
	if len(consumers) == 0 {
		return nil
	}

	event := MethodCallEvent{
		Consumer: consumers[0],
		Method:   method,
		Host:     pr.Addr.String(),
	}

	i.publisher.Notify(event)
	return nil
}

func asyncServe(ctx context.Context, server *grpc.Server, lis net.Listener) {
	go func() {
		if err := server.Serve(lis); err != nil {
			log.Printf("failed to serve grpc server: %v", err)
		}
		fmt.Println("server was successfully shutdown!")
	}()

	select {
	case <-ctx.Done():
		log.Println("context fired, start to graceful server shutdown...")
		server.GracefulStop()
	}
}

func StartMyMicroservice(ctx context.Context, addr, aclStr string) error {
	const consumerKey = "consumer"

	publisher := NewMethodCallPublisher()
	aclManager := &ACLManager{}
	if err := aclManager.Load([]byte(aclStr)); err != nil {
		return err
	}

	aclInterceptor := &ACLInterceptor{
		consumerKey: consumerKey,
		manager:     aclManager,
	}
	mcInterceptor := &MethodCallNotifyInterceptor{
		consumerKey: consumerKey,
		publisher:   publisher,
	}

	server := grpc.NewServer(
		grpc.ChainUnaryInterceptor(
			aclInterceptor.Unary(),
			mcInterceptor.Unary(),
		),
		grpc.ChainStreamInterceptor(
			aclInterceptor.Stream(),
			mcInterceptor.Stream(),
		),
	)
	RegisterBizServer(server, &BizManager{})
	RegisterAdminServer(server, &AdminManager{publisher: publisher})

	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to listen tcp socket %s: %v", addr, err)
	}

	go asyncServe(ctx, server, lis)
	return nil
}

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

type Event struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Timestamp int64  `protobuf:"varint,1,opt,name=timestamp,proto3" json:"timestamp,omitempty"`
	Consumer  string `protobuf:"bytes,2,opt,name=consumer,proto3" json:"consumer,omitempty"`
	Method    string `protobuf:"bytes,3,opt,name=method,proto3" json:"method,omitempty"`
	Host      string `protobuf:"bytes,4,opt,name=host,proto3" json:"host,omitempty"`
}

func (x *Event) Reset() {
	*x = Event{}
	if protoimpl.UnsafeEnabled {
		mi := &file_service_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Event) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Event) ProtoMessage() {}

func (x *Event) ProtoReflect() protoreflect.Message {
	mi := &file_service_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Event.ProtoReflect.Descriptor instead.
func (*Event) Descriptor() ([]byte, []int) {
	return file_service_proto_rawDescGZIP(), []int{0}
}

func (x *Event) GetTimestamp() int64 {
	if x != nil {
		return x.Timestamp
	}
	return 0
}

func (x *Event) GetConsumer() string {
	if x != nil {
		return x.Consumer
	}
	return ""
}

func (x *Event) GetMethod() string {
	if x != nil {
		return x.Method
	}
	return ""
}

func (x *Event) GetHost() string {
	if x != nil {
		return x.Host
	}
	return ""
}

type Stat struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Timestamp  int64             `protobuf:"varint,1,opt,name=timestamp,proto3" json:"timestamp,omitempty"`
	ByMethod   map[string]uint64 `protobuf:"bytes,2,rep,name=by_method,json=byMethod,proto3" json:"by_method,omitempty" protobuf_key:"bytes,1,opt,name=key,proto3" protobuf_val:"varint,2,opt,name=value,proto3"`
	ByConsumer map[string]uint64 `protobuf:"bytes,3,rep,name=by_consumer,json=byConsumer,proto3" json:"by_consumer,omitempty" protobuf_key:"bytes,1,opt,name=key,proto3" protobuf_val:"varint,2,opt,name=value,proto3"`
}

func (x *Stat) Reset() {
	*x = Stat{}
	if protoimpl.UnsafeEnabled {
		mi := &file_service_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Stat) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Stat) ProtoMessage() {}

func (x *Stat) ProtoReflect() protoreflect.Message {
	mi := &file_service_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Stat.ProtoReflect.Descriptor instead.
func (*Stat) Descriptor() ([]byte, []int) {
	return file_service_proto_rawDescGZIP(), []int{1}
}

func (x *Stat) GetTimestamp() int64 {
	if x != nil {
		return x.Timestamp
	}
	return 0
}

func (x *Stat) GetByMethod() map[string]uint64 {
	if x != nil {
		return x.ByMethod
	}
	return nil
}

func (x *Stat) GetByConsumer() map[string]uint64 {
	if x != nil {
		return x.ByConsumer
	}
	return nil
}

type StatInterval struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	IntervalSeconds uint64 `protobuf:"varint,1,opt,name=interval_seconds,json=intervalSeconds,proto3" json:"interval_seconds,omitempty"`
}

func (x *StatInterval) Reset() {
	*x = StatInterval{}
	if protoimpl.UnsafeEnabled {
		mi := &file_service_proto_msgTypes[2]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *StatInterval) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*StatInterval) ProtoMessage() {}

func (x *StatInterval) ProtoReflect() protoreflect.Message {
	mi := &file_service_proto_msgTypes[2]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use StatInterval.ProtoReflect.Descriptor instead.
func (*StatInterval) Descriptor() ([]byte, []int) {
	return file_service_proto_rawDescGZIP(), []int{2}
}

func (x *StatInterval) GetIntervalSeconds() uint64 {
	if x != nil {
		return x.IntervalSeconds
	}
	return 0
}

type Nothing struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Dummy bool `protobuf:"varint,1,opt,name=dummy,proto3" json:"dummy,omitempty"`
}

func (x *Nothing) Reset() {
	*x = Nothing{}
	if protoimpl.UnsafeEnabled {
		mi := &file_service_proto_msgTypes[3]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Nothing) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Nothing) ProtoMessage() {}

func (x *Nothing) ProtoReflect() protoreflect.Message {
	mi := &file_service_proto_msgTypes[3]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Nothing.ProtoReflect.Descriptor instead.
func (*Nothing) Descriptor() ([]byte, []int) {
	return file_service_proto_rawDescGZIP(), []int{3}
}

func (x *Nothing) GetDummy() bool {
	if x != nil {
		return x.Dummy
	}
	return false
}

var File_service_proto protoreflect.FileDescriptor

var file_service_proto_rawDesc = []byte{
	0x0a, 0x0d, 0x73, 0x65, 0x72, 0x76, 0x69, 0x63, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12,
	0x04, 0x6d, 0x61, 0x69, 0x6e, 0x22, 0x6d, 0x0a, 0x05, 0x45, 0x76, 0x65, 0x6e, 0x74, 0x12, 0x1c,
	0x0a, 0x09, 0x74, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70, 0x18, 0x01, 0x20, 0x01, 0x28,
	0x03, 0x52, 0x09, 0x74, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70, 0x12, 0x1a, 0x0a, 0x08,
	0x63, 0x6f, 0x6e, 0x73, 0x75, 0x6d, 0x65, 0x72, 0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x08,
	0x63, 0x6f, 0x6e, 0x73, 0x75, 0x6d, 0x65, 0x72, 0x12, 0x16, 0x0a, 0x06, 0x6d, 0x65, 0x74, 0x68,
	0x6f, 0x64, 0x18, 0x03, 0x20, 0x01, 0x28, 0x09, 0x52, 0x06, 0x6d, 0x65, 0x74, 0x68, 0x6f, 0x64,
	0x12, 0x12, 0x0a, 0x04, 0x68, 0x6f, 0x73, 0x74, 0x18, 0x04, 0x20, 0x01, 0x28, 0x09, 0x52, 0x04,
	0x68, 0x6f, 0x73, 0x74, 0x22, 0x94, 0x02, 0x0a, 0x04, 0x53, 0x74, 0x61, 0x74, 0x12, 0x1c, 0x0a,
	0x09, 0x74, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70, 0x18, 0x01, 0x20, 0x01, 0x28, 0x03,
	0x52, 0x09, 0x74, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70, 0x12, 0x35, 0x0a, 0x09, 0x62,
	0x79, 0x5f, 0x6d, 0x65, 0x74, 0x68, 0x6f, 0x64, 0x18, 0x02, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x18,
	0x2e, 0x6d, 0x61, 0x69, 0x6e, 0x2e, 0x53, 0x74, 0x61, 0x74, 0x2e, 0x42, 0x79, 0x4d, 0x65, 0x74,
	0x68, 0x6f, 0x64, 0x45, 0x6e, 0x74, 0x72, 0x79, 0x52, 0x08, 0x62, 0x79, 0x4d, 0x65, 0x74, 0x68,
	0x6f, 0x64, 0x12, 0x3b, 0x0a, 0x0b, 0x62, 0x79, 0x5f, 0x63, 0x6f, 0x6e, 0x73, 0x75, 0x6d, 0x65,
	0x72, 0x18, 0x03, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x1a, 0x2e, 0x6d, 0x61, 0x69, 0x6e, 0x2e, 0x53,
	0x74, 0x61, 0x74, 0x2e, 0x42, 0x79, 0x43, 0x6f, 0x6e, 0x73, 0x75, 0x6d, 0x65, 0x72, 0x45, 0x6e,
	0x74, 0x72, 0x79, 0x52, 0x0a, 0x62, 0x79, 0x43, 0x6f, 0x6e, 0x73, 0x75, 0x6d, 0x65, 0x72, 0x1a,
	0x3b, 0x0a, 0x0d, 0x42, 0x79, 0x4d, 0x65, 0x74, 0x68, 0x6f, 0x64, 0x45, 0x6e, 0x74, 0x72, 0x79,
	0x12, 0x10, 0x0a, 0x03, 0x6b, 0x65, 0x79, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x03, 0x6b,
	0x65, 0x79, 0x12, 0x14, 0x0a, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x18, 0x02, 0x20, 0x01, 0x28,
	0x04, 0x52, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x3a, 0x02, 0x38, 0x01, 0x1a, 0x3d, 0x0a, 0x0f,
	0x42, 0x79, 0x43, 0x6f, 0x6e, 0x73, 0x75, 0x6d, 0x65, 0x72, 0x45, 0x6e, 0x74, 0x72, 0x79, 0x12,
	0x10, 0x0a, 0x03, 0x6b, 0x65, 0x79, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x03, 0x6b, 0x65,
	0x79, 0x12, 0x14, 0x0a, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x18, 0x02, 0x20, 0x01, 0x28, 0x04,
	0x52, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x3a, 0x02, 0x38, 0x01, 0x22, 0x39, 0x0a, 0x0c, 0x53,
	0x74, 0x61, 0x74, 0x49, 0x6e, 0x74, 0x65, 0x72, 0x76, 0x61, 0x6c, 0x12, 0x29, 0x0a, 0x10, 0x69,
	0x6e, 0x74, 0x65, 0x72, 0x76, 0x61, 0x6c, 0x5f, 0x73, 0x65, 0x63, 0x6f, 0x6e, 0x64, 0x73, 0x18,
	0x01, 0x20, 0x01, 0x28, 0x04, 0x52, 0x0f, 0x69, 0x6e, 0x74, 0x65, 0x72, 0x76, 0x61, 0x6c, 0x53,
	0x65, 0x63, 0x6f, 0x6e, 0x64, 0x73, 0x22, 0x1f, 0x0a, 0x07, 0x4e, 0x6f, 0x74, 0x68, 0x69, 0x6e,
	0x67, 0x12, 0x14, 0x0a, 0x05, 0x64, 0x75, 0x6d, 0x6d, 0x79, 0x18, 0x01, 0x20, 0x01, 0x28, 0x08,
	0x52, 0x05, 0x64, 0x75, 0x6d, 0x6d, 0x79, 0x32, 0x64, 0x0a, 0x05, 0x41, 0x64, 0x6d, 0x69, 0x6e,
	0x12, 0x29, 0x0a, 0x07, 0x4c, 0x6f, 0x67, 0x67, 0x69, 0x6e, 0x67, 0x12, 0x0d, 0x2e, 0x6d, 0x61,
	0x69, 0x6e, 0x2e, 0x4e, 0x6f, 0x74, 0x68, 0x69, 0x6e, 0x67, 0x1a, 0x0b, 0x2e, 0x6d, 0x61, 0x69,
	0x6e, 0x2e, 0x45, 0x76, 0x65, 0x6e, 0x74, 0x22, 0x00, 0x30, 0x01, 0x12, 0x30, 0x0a, 0x0a, 0x53,
	0x74, 0x61, 0x74, 0x69, 0x73, 0x74, 0x69, 0x63, 0x73, 0x12, 0x12, 0x2e, 0x6d, 0x61, 0x69, 0x6e,
	0x2e, 0x53, 0x74, 0x61, 0x74, 0x49, 0x6e, 0x74, 0x65, 0x72, 0x76, 0x61, 0x6c, 0x1a, 0x0a, 0x2e,
	0x6d, 0x61, 0x69, 0x6e, 0x2e, 0x53, 0x74, 0x61, 0x74, 0x22, 0x00, 0x30, 0x01, 0x32, 0x7d, 0x0a,
	0x03, 0x42, 0x69, 0x7a, 0x12, 0x27, 0x0a, 0x05, 0x43, 0x68, 0x65, 0x63, 0x6b, 0x12, 0x0d, 0x2e,
	0x6d, 0x61, 0x69, 0x6e, 0x2e, 0x4e, 0x6f, 0x74, 0x68, 0x69, 0x6e, 0x67, 0x1a, 0x0d, 0x2e, 0x6d,
	0x61, 0x69, 0x6e, 0x2e, 0x4e, 0x6f, 0x74, 0x68, 0x69, 0x6e, 0x67, 0x22, 0x00, 0x12, 0x25, 0x0a,
	0x03, 0x41, 0x64, 0x64, 0x12, 0x0d, 0x2e, 0x6d, 0x61, 0x69, 0x6e, 0x2e, 0x4e, 0x6f, 0x74, 0x68,
	0x69, 0x6e, 0x67, 0x1a, 0x0d, 0x2e, 0x6d, 0x61, 0x69, 0x6e, 0x2e, 0x4e, 0x6f, 0x74, 0x68, 0x69,
	0x6e, 0x67, 0x22, 0x00, 0x12, 0x26, 0x0a, 0x04, 0x54, 0x65, 0x73, 0x74, 0x12, 0x0d, 0x2e, 0x6d,
	0x61, 0x69, 0x6e, 0x2e, 0x4e, 0x6f, 0x74, 0x68, 0x69, 0x6e, 0x67, 0x1a, 0x0d, 0x2e, 0x6d, 0x61,
	0x69, 0x6e, 0x2e, 0x4e, 0x6f, 0x74, 0x68, 0x69, 0x6e, 0x67, 0x22, 0x00, 0x42, 0x08, 0x5a, 0x06,
	0x2e, 0x3b, 0x6d, 0x61, 0x69, 0x6e, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_service_proto_rawDescOnce sync.Once
	file_service_proto_rawDescData = file_service_proto_rawDesc
)

func file_service_proto_rawDescGZIP() []byte {
	file_service_proto_rawDescOnce.Do(func() {
		file_service_proto_rawDescData = protoimpl.X.CompressGZIP(file_service_proto_rawDescData)
	})
	return file_service_proto_rawDescData
}

var file_service_proto_msgTypes = make([]protoimpl.MessageInfo, 6)
var file_service_proto_goTypes = []interface{}{
	(*Event)(nil),        // 0: main.Event
	(*Stat)(nil),         // 1: main.Stat
	(*StatInterval)(nil), // 2: main.StatInterval
	(*Nothing)(nil),      // 3: main.Nothing
	nil,                  // 4: main.Stat.ByMethodEntry
	nil,                  // 5: main.Stat.ByConsumerEntry
}
var file_service_proto_depIdxs = []int32{
	4, // 0: main.Stat.by_method:type_name -> main.Stat.ByMethodEntry
	5, // 1: main.Stat.by_consumer:type_name -> main.Stat.ByConsumerEntry
	3, // 2: main.Admin.Logging:input_type -> main.Nothing
	2, // 3: main.Admin.Statistics:input_type -> main.StatInterval
	3, // 4: main.Biz.Check:input_type -> main.Nothing
	3, // 5: main.Biz.Add:input_type -> main.Nothing
	3, // 6: main.Biz.Test:input_type -> main.Nothing
	0, // 7: main.Admin.Logging:output_type -> main.Event
	1, // 8: main.Admin.Statistics:output_type -> main.Stat
	3, // 9: main.Biz.Check:output_type -> main.Nothing
	3, // 10: main.Biz.Add:output_type -> main.Nothing
	3, // 11: main.Biz.Test:output_type -> main.Nothing
	7, // [7:12] is the sub-list for method output_type
	2, // [2:7] is the sub-list for method input_type
	2, // [2:2] is the sub-list for extension type_name
	2, // [2:2] is the sub-list for extension extendee
	0, // [0:2] is the sub-list for field type_name
}

func init() { file_service_proto_init() }
func file_service_proto_init() {
	if File_service_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_service_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*Event); i {
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
		file_service_proto_msgTypes[1].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*Stat); i {
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
		file_service_proto_msgTypes[2].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*StatInterval); i {
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
		file_service_proto_msgTypes[3].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*Nothing); i {
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
			RawDescriptor: file_service_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   6,
			NumExtensions: 0,
			NumServices:   2,
		},
		GoTypes:           file_service_proto_goTypes,
		DependencyIndexes: file_service_proto_depIdxs,
		MessageInfos:      file_service_proto_msgTypes,
	}.Build()
	File_service_proto = out.File
	file_service_proto_rawDesc = nil
	file_service_proto_goTypes = nil
	file_service_proto_depIdxs = nil
}

// Reference imports to suppress errors if they are not otherwise used.
var _ context.Context
var _ grpc.ClientConnInterface

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
const _ = grpc.SupportPackageIsVersion6

// AdminClient is the client API for Admin service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://godoc.org/google.golang.org/grpc#ClientConn.NewStream.
type AdminClient interface {
	Logging(ctx context.Context, in *Nothing, opts ...grpc.CallOption) (Admin_LoggingClient, error)
	Statistics(ctx context.Context, in *StatInterval, opts ...grpc.CallOption) (Admin_StatisticsClient, error)
}

type adminClient struct {
	cc grpc.ClientConnInterface
}

func NewAdminClient(cc grpc.ClientConnInterface) AdminClient {
	return &adminClient{cc}
}

func (c *adminClient) Logging(ctx context.Context, in *Nothing, opts ...grpc.CallOption) (Admin_LoggingClient, error) {
	stream, err := c.cc.NewStream(ctx, &_Admin_serviceDesc.Streams[0], "/main.Admin/Logging", opts...)
	if err != nil {
		return nil, err
	}
	x := &adminLoggingClient{stream}
	if err := x.ClientStream.SendMsg(in); err != nil {
		return nil, err
	}
	if err := x.ClientStream.CloseSend(); err != nil {
		return nil, err
	}
	return x, nil
}

type Admin_LoggingClient interface {
	Recv() (*Event, error)
	grpc.ClientStream
}

type adminLoggingClient struct {
	grpc.ClientStream
}

func (x *adminLoggingClient) Recv() (*Event, error) {
	m := new(Event)
	if err := x.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

func (c *adminClient) Statistics(ctx context.Context, in *StatInterval, opts ...grpc.CallOption) (Admin_StatisticsClient, error) {
	stream, err := c.cc.NewStream(ctx, &_Admin_serviceDesc.Streams[1], "/main.Admin/Statistics", opts...)
	if err != nil {
		return nil, err
	}
	x := &adminStatisticsClient{stream}
	if err := x.ClientStream.SendMsg(in); err != nil {
		return nil, err
	}
	if err := x.ClientStream.CloseSend(); err != nil {
		return nil, err
	}
	return x, nil
}

type Admin_StatisticsClient interface {
	Recv() (*Stat, error)
	grpc.ClientStream
}

type adminStatisticsClient struct {
	grpc.ClientStream
}

func (x *adminStatisticsClient) Recv() (*Stat, error) {
	m := new(Stat)
	if err := x.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

// AdminServer is the server API for Admin service.
type AdminServer interface {
	Logging(*Nothing, Admin_LoggingServer) error
	Statistics(*StatInterval, Admin_StatisticsServer) error
}

// UnimplementedAdminServer can be embedded to have forward compatible implementations.
type UnimplementedAdminServer struct {
}

func (*UnimplementedAdminServer) Logging(*Nothing, Admin_LoggingServer) error {
	return status.Errorf(codes.Unimplemented, "method Logging not implemented")
}
func (*UnimplementedAdminServer) Statistics(*StatInterval, Admin_StatisticsServer) error {
	return status.Errorf(codes.Unimplemented, "method Statistics not implemented")
}

func RegisterAdminServer(s *grpc.Server, srv AdminServer) {
	s.RegisterService(&_Admin_serviceDesc, srv)
}

func _Admin_Logging_Handler(srv interface{}, stream grpc.ServerStream) error {
	m := new(Nothing)
	if err := stream.RecvMsg(m); err != nil {
		return err
	}
	return srv.(AdminServer).Logging(m, &adminLoggingServer{stream})
}

type Admin_LoggingServer interface {
	Send(*Event) error
	grpc.ServerStream
}

type adminLoggingServer struct {
	grpc.ServerStream
}

func (x *adminLoggingServer) Send(m *Event) error {
	return x.ServerStream.SendMsg(m)
}

func _Admin_Statistics_Handler(srv interface{}, stream grpc.ServerStream) error {
	m := new(StatInterval)
	if err := stream.RecvMsg(m); err != nil {
		return err
	}
	return srv.(AdminServer).Statistics(m, &adminStatisticsServer{stream})
}

type Admin_StatisticsServer interface {
	Send(*Stat) error
	grpc.ServerStream
}

type adminStatisticsServer struct {
	grpc.ServerStream
}

func (x *adminStatisticsServer) Send(m *Stat) error {
	return x.ServerStream.SendMsg(m)
}

var _Admin_serviceDesc = grpc.ServiceDesc{
	ServiceName: "main.Admin",
	HandlerType: (*AdminServer)(nil),
	Methods:     []grpc.MethodDesc{},
	Streams: []grpc.StreamDesc{
		{
			StreamName:    "Logging",
			Handler:       _Admin_Logging_Handler,
			ServerStreams: true,
		},
		{
			StreamName:    "Statistics",
			Handler:       _Admin_Statistics_Handler,
			ServerStreams: true,
		},
	},
	Metadata: "service.proto",
}

// BizClient is the client API for Biz service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://godoc.org/google.golang.org/grpc#ClientConn.NewStream.
type BizClient interface {
	Check(ctx context.Context, in *Nothing, opts ...grpc.CallOption) (*Nothing, error)
	Add(ctx context.Context, in *Nothing, opts ...grpc.CallOption) (*Nothing, error)
	Test(ctx context.Context, in *Nothing, opts ...grpc.CallOption) (*Nothing, error)
}

type bizClient struct {
	cc grpc.ClientConnInterface
}

func NewBizClient(cc grpc.ClientConnInterface) BizClient {
	return &bizClient{cc}
}

func (c *bizClient) Check(ctx context.Context, in *Nothing, opts ...grpc.CallOption) (*Nothing, error) {
	out := new(Nothing)
	err := c.cc.Invoke(ctx, "/main.Biz/Check", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *bizClient) Add(ctx context.Context, in *Nothing, opts ...grpc.CallOption) (*Nothing, error) {
	out := new(Nothing)
	err := c.cc.Invoke(ctx, "/main.Biz/Add", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *bizClient) Test(ctx context.Context, in *Nothing, opts ...grpc.CallOption) (*Nothing, error) {
	out := new(Nothing)
	err := c.cc.Invoke(ctx, "/main.Biz/Test", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// BizServer is the server API for Biz service.
type BizServer interface {
	Check(context.Context, *Nothing) (*Nothing, error)
	Add(context.Context, *Nothing) (*Nothing, error)
	Test(context.Context, *Nothing) (*Nothing, error)
}

// UnimplementedBizServer can be embedded to have forward compatible implementations.
type UnimplementedBizServer struct {
}

func (*UnimplementedBizServer) Check(context.Context, *Nothing) (*Nothing, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Check not implemented")
}
func (*UnimplementedBizServer) Add(context.Context, *Nothing) (*Nothing, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Add not implemented")
}
func (*UnimplementedBizServer) Test(context.Context, *Nothing) (*Nothing, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Test not implemented")
}

func RegisterBizServer(s *grpc.Server, srv BizServer) {
	s.RegisterService(&_Biz_serviceDesc, srv)
}

func _Biz_Check_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(Nothing)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(BizServer).Check(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/main.Biz/Check",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(BizServer).Check(ctx, req.(*Nothing))
	}
	return interceptor(ctx, in, info, handler)
}

func _Biz_Add_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(Nothing)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(BizServer).Add(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/main.Biz/Add",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(BizServer).Add(ctx, req.(*Nothing))
	}
	return interceptor(ctx, in, info, handler)
}

func _Biz_Test_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(Nothing)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(BizServer).Test(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/main.Biz/Test",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(BizServer).Test(ctx, req.(*Nothing))
	}
	return interceptor(ctx, in, info, handler)
}

var _Biz_serviceDesc = grpc.ServiceDesc{
	ServiceName: "main.Biz",
	HandlerType: (*BizServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "Check",
			Handler:    _Biz_Check_Handler,
		},
		{
			MethodName: "Add",
			Handler:    _Biz_Add_Handler,
		},
		{
			MethodName: "Test",
			Handler:    _Biz_Test_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "service.proto",
}
