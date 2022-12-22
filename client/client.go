package main

import (
	"context"
	"errors"
	"math/rand"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "github.com/dylan-p-wong/kvstore/api"
)

type Client struct {
	leader     int
	config     ClientConfig
	connection *grpc.ClientConn
}

func NewClient(config ClientConfig) (*Client, error) {
	return &Client{leader: -1, config: config}, nil
}

func (c *Client) GetRandomServer() int {
	keys := make([]int, 0)

	for k := range c.config.Servers {
		keys = append(keys, k)
	}

	return keys[rand.Intn(len(keys))]
}

func (c *Client) GetNewConnection() (*grpc.ClientConn, error) {
	if c.leader == -1 {
		c.leader = c.GetRandomServer()
	}
	if c.config.Servers[c.leader] == "" {
		c.leader = -1
		return nil, errors.New("invalid leader")
	}
	connection, err := grpc.Dial(c.config.Servers[c.leader], grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		c.leader = -1
		return nil, err
	}
	return connection, nil
}

func (c *Client) GetProtoClient() (pb.KVClient, error) {
	if c.connection == nil {
		conn, err := c.GetNewConnection()
		if err != nil {
			return nil, err
		}
		c.connection = conn
	}

	return pb.NewKVClient(c.connection), nil
}

func (c *Client) Get(key string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	client, err := c.GetProtoClient()
	if err != nil {
		return "", err
	}

	gr, err := client.Get(ctx, &pb.GetRequest{Key: []byte(key)})

	if err == grpc.ErrServerStopped || err == grpc.ErrClientConnTimeout || err == grpc.ErrClientConnClosing {
		c.connection.Close()
		c.connection = nil
		return "", err
	}

	if err != nil {
		if strings.Index(err.Error(), "not leader") != -1 {
			c.connection.Close()
			c.connection = nil
			c.leader = int(gr.GetLeader())
			return "", err
		}
		return "", err
	}

	return string(gr.GetValue()), nil
}

func (c *Client) Put(key string, value string) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	client, err := c.GetProtoClient()
	if err != nil {
		return err
	}

	pr, err := client.Put(ctx, &pb.PutRequest{Key: []byte(key), Value: []byte(value)})

	if err == grpc.ErrServerStopped || err == grpc.ErrClientConnTimeout || err == grpc.ErrClientConnClosing {
		c.connection.Close()
		c.connection = nil
		return err
	}

	if err != nil {
		if strings.Index(err.Error(), "not leader") != -1 {
			c.connection.Close()
			c.connection = nil
			c.leader = int(pr.GetLeader())
			return err
		}
		return err
	}

	return nil
}

func (c *Client) Delete(key string) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	client, err := c.GetProtoClient()
	if err != nil {
		return err
	}

	dr, err := client.Delete(ctx, &pb.DeleteRequest{Key: []byte(key)})

	if err == grpc.ErrServerStopped || err == grpc.ErrClientConnTimeout || err == grpc.ErrClientConnClosing {
		c.connection.Close()
		c.connection = nil
		return err
	}

	if err != nil {
		if strings.Index(err.Error(), "not leader") != -1 {
			c.connection.Close()
			c.connection = nil
			c.leader = int(dr.GetLeader())
			return err
		}
		return err
	}

	return nil
}
