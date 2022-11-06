package service

import (
	"os"

	pb "github.com/dylan-p-wong/kvstore/api"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type ClientConnection struct {
	clientConnection *grpc.ClientConn
	client           pb.KVClient
}

type Transport struct {
	rpcChannel chan RPCRequest
	peers      map[string]*ClientConnection
}

func NewTransport() Transport {
	return Transport{
		rpcChannel: make(chan RPCRequest),
		peers:      make(map[string]*ClientConnection),
	}
}

func (t *Transport) GetPeer(name string, peerUrl string) (pb.KVClient, error) {
	c, ok := t.peers[name]

	if !ok {
		c = &ClientConnection{}
	}

	if c.clientConnection == nil {
		conn, err := grpc.Dial(peerUrl, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return nil, err
		}

		c.clientConnection = conn
		c.client = pb.NewKVClient(conn)
	}

	return c.client, nil
}

func (t *Transport) RPCWorker() {
	for {
		rpc := <-t.rpcChannel

		switch rpc.Command.(type) {
		case *pb.AppendEntriesRequest:
			rpc.ResponseChannel <- RPCResponse{
				Response: &pb.AppendEntriesResponse{
					Term:    rpc.Command.(*pb.AppendEntriesRequest).Term,
					Success: true,
				},
				Error: nil,
			}
		case *pb.RequestVoteRequest:
			rpc.ResponseChannel <- RPCResponse{
				Response: &pb.RequestVoteResponse{
					Term:        rpc.Command.(*pb.RequestVoteRequest).Term,
					VoteGranted: true,
				},
				Error: nil,
			}
		default:
			os.Exit(1)
		}
	}
}
