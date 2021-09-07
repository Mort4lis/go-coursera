package main

import (
	"context"
	"fmt"
	"google.golang.org/grpc"
	"log"
	"net"
)

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

func StartMyMicroservice(ctx context.Context, addr, acl string) error {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to listen tcp socket %s: %v", addr, err)
	}

	server := grpc.NewServer()
	go asyncServe(ctx, server, lis)
	return nil
}
