package main

import (
	"context"
	"flag"
	"log"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	pb "github.com/dylan-p-wong/kvstore/api"
)

var (
	addr = flag.String("addr", "127.0.0.1:4444", "the address to connect to")
)

func main() {
	flag.Parse()
	
	// Set up a connection to the server.
	conn, err := grpc.Dial(*addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	c := pb.NewKVClient(conn)

	// Contact the server and print out its response.
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	pr, err := c.Put(ctx, &pb.PutRequest{Key: []byte("key"), Value: []byte("value")})
	if err != nil {
		log.Fatalf("could not greet: %v", err)
	}
	log.Printf("KV: %t", pr.GetSuccess())

	// gr, err := c.Get(ctx, &pb.GetRequest{Key: []byte("key")})
	// if err != nil {
	// 	log.Fatalf("could not greet: %v", err)
	// }
	// log.Printf("KV: (%s, %s)", gr.GetKey(), gr.GetValue())
}
