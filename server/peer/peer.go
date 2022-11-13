package peer

import (
	"context"
	"time"

	pb "github.com/dylan-p-wong/kvstore/api"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Peer struct {
	id                int
	url               string
	clientConnection  *grpc.ClientConn
	client            pb.KVClient
	heartbeatInterval time.Duration

	stopChannel chan bool
}

func New(id int, url string, heartbeatInterval time.Duration) (*Peer, error) {
	connection, err := grpc.Dial(url, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}

	return &Peer{
		id:                id,
		url:               url,
		clientConnection:  connection,
		client:            pb.NewKVClient(connection),
		heartbeatInterval: heartbeatInterval,
	}, nil
}

func (p *Peer) SendVoteRequest(request *pb.RequestVoteRequest, requestVoteResponseChannel chan *pb.RequestVoteResponse) {
	response, err := p.client.RequestVote(context.Background(), request)

	// TODO

	if err == nil {
		requestVoteResponseChannel <- response
	}
}

func (p *Peer) SendAppendEntriesRequest() {
	// send request

	// if success update the prev log index

	// if fail handle

	// send to event loop for processing. should send async to not block like the others
}

func (p *Peer) StartHeartbeat() {
	
}

func (p *Peer) StopHeartbeat() {

}

func (p *Peer) Flush() {
	
}

// Listens to the heartbeat timeout and flushes an AppendEntries RPC
func (p *Peer) Heartbeat(c chan bool) {
	
}
