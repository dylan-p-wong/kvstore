package main

import (
	"bufio"
	"context"
	"flag"
	"log"
	"os"
	"strings"
	"time"

	pb "github.com/dylan-p-wong/kvstore/api"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
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

	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		s := strings.Split(scanner.Text(), " ")

		if len(s) == 0 {
			log.Printf("invalid command")
			continue
		}

		op := s[0]

		if op == "GET" {
			if len(s) != 2 {
				log.Printf("invalid number of args")
				continue
			}

			key := s[1]

			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()

			gr, err := c.Get(ctx, &pb.GetRequest{Key: []byte(key)})
			if err != nil {
				log.Printf("could not get: %v", err)
			} else {
				log.Printf("get: (%s, %s)", gr.GetKey(), gr.GetValue())
			}
		} else if op == "PUT" {
			if len(s) != 3 {
				log.Printf("invalid number of args")
				continue
			}

			key := s[1]
			value := s[2]

			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()

			pr, err := c.Put(ctx, &pb.PutRequest{Key: []byte(key), Value: []byte(value)})
			if err != nil {
				log.Printf("could not put: %v", err)
			} else {
				log.Printf("put: %t", pr.GetSuccess())
			}
		} else if op == "DELETE" {
			if len(s) != 2 {
				log.Printf("invalid number of args")
				continue
			}

			key := s[1]

			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()

			pr, err := c.Delete(ctx, &pb.DeleteRequest{Key: []byte(key)})
			if err != nil {
				log.Printf("could not delete: %v", err)
			} else {
				log.Printf("delete: %t", pr.GetSuccess())
			}
		} else {
			log.Printf("invalid operation")
			continue
		}
	}
}
