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
	"log"
	"net"
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
