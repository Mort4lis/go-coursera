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
	stream.Send(&Stat{
		Timestamp:  0,
		ByMethod:   nil,
		ByConsumer: nil,
	})
	return nil
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

type ACLMethodManager struct {
	mdKey   string
	manager *ACLManager
}

func (i *ACLMethodManager) HasAccess(ctx context.Context, method string) bool {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return false
	}

	for _, k := range md.Get(i.mdKey) {
		if i.manager.HasAccess(k, method) {
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
	p.subscribers[subscriberID] = make(chan MethodCallEvent)

	return p.subscribers[subscriberID], subscriberID
}

func (p *MethodCallPublisher) Unsubscribe(subscribeID int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.subscribers, subscribeID)
}

func aclUnaryInterceptor(manager *ACLMethodManager) grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req interface{},
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (resp interface{}, err error) {
		if manager.HasAccess(ctx, info.FullMethod) {
			return handler(ctx, req)
		}
		return nil, status.Error(
			codes.Unauthenticated,
			"consumer doesn't have permissions to call this method",
		)
	}
}

func aclStreamInterceptor(manager *ACLMethodManager) grpc.StreamServerInterceptor {
	return func(
		srv interface{},
		stream grpc.ServerStream,
		info *grpc.StreamServerInfo,
		handler grpc.StreamHandler,
	) error {
		if manager.HasAccess(stream.Context(), info.FullMethod) {
			return handler(srv, stream)
		}
		return status.Error(
			codes.Unauthenticated,
			"consumer doesn't have permissions to call this method",
		)
	}
}

func notifyUnaryMethodCallInterceptor(publisher *MethodCallPublisher) grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req interface{},
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (resp interface{}, err error) {
		md, ok := metadata.FromIncomingContext(ctx)
		if !ok {
			return handler(ctx, req)
		}

		pr, ok := peer.FromContext(ctx)
		if !ok {
			return handler(ctx, req)
		}

		consumers := md.Get("consumer")
		if len(consumers) == 0 {
			return handler(ctx, req)
		}

		event := MethodCallEvent{
			Consumer: consumers[0],
			Method:   info.FullMethod,
			Host:     pr.Addr.String(),
		}
		publisher.Notify(event)

		return handler(ctx, req)
	}
}

func notifyStreamMethodCallInterceptor(publisher *MethodCallPublisher) grpc.StreamServerInterceptor {
	return func(
		srv interface{},
		stream grpc.ServerStream,
		info *grpc.StreamServerInfo,
		handler grpc.StreamHandler,
	) error {
		md, ok := metadata.FromIncomingContext(stream.Context())
		if !ok {
			return handler(srv, stream)
		}

		pr, ok := peer.FromContext(stream.Context())
		if !ok {
			return handler(srv, stream)
		}

		consumers := md.Get("consumer")
		if len(consumers) == 0 {
			return handler(srv, stream)
		}

		event := MethodCallEvent{
			Consumer: consumers[0],
			Method:   info.FullMethod,
			Host:     pr.Addr.String(),
		}
		publisher.Notify(event)

		return handler(srv, stream)
	}
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
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to listen tcp socket %s: %v", addr, err)
	}

	aclManager := &ACLManager{}
	if err = aclManager.Load([]byte(aclStr)); err != nil {
		return err
	}
	aclMethodManager := &ACLMethodManager{
		mdKey:   "consumer",
		manager: aclManager,
	}

	publisher := NewMethodCallPublisher()

	server := grpc.NewServer(
		grpc.ChainUnaryInterceptor(
			aclUnaryInterceptor(aclMethodManager),
			notifyUnaryMethodCallInterceptor(publisher),
		),
		grpc.ChainStreamInterceptor(
			aclStreamInterceptor(aclMethodManager),
			notifyStreamMethodCallInterceptor(publisher),
		),
	)
	RegisterBizServer(server, &BizManager{})
	RegisterAdminServer(server, &AdminManager{publisher: publisher})

	go asyncServe(ctx, server, lis)
	return nil
}
