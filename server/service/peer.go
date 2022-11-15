package service

import (
	"context"
	"time"

	pb "github.com/dylan-p-wong/kvstore/api"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type peer struct {
	id                int
	url               string
	clientConnection  *grpc.ClientConn
	client            pb.KVClient
	heartbeatInterval time.Duration

	stopChannel chan bool
	server      *Server
}

func NewPeer(id int, url string, heartbeatInterval time.Duration, server *Server) (*peer, error) {
	connection, err := grpc.Dial(url, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		server.sugar.Infow("error dialing connection", "err", err)
		return nil, err
	}

	return &peer{
		id:                id,
		url:               url,
		clientConnection:  connection,
		client:            pb.NewKVClient(connection),
		heartbeatInterval: heartbeatInterval,
		server:            server,
	}, nil
}

func (p *peer) SendVoteRequest(request *pb.RequestVoteRequest, requestVoteResponseChannel chan *pb.RequestVoteResponse) {
	p.server.sugar.Infow("sending request vote request", "peer", p.id)
	response, err := p.client.RequestVote(context.Background(), request)

	if err == nil {
		p.server.sugar.Infow("success sending request vote request", "response", response)
		requestVoteResponseChannel <- response
	} else {
		p.server.sugar.Infow("error sending request vote request", "err", err)
	}
}

func (p *peer) SendAppendEntriesRequest(request *pb.AppendEntriesRequest) {
	p.server.sugar.Infow("sending append entries request", "peer", p.id, "request", request)
	response, err := p.client.AppendEntries(context.Background(), request)

	if err != nil {
		p.server.sugar.Infow("error sending append entries request", "err", err)
		return
	}

	// TODO

	// run in go routine so we do not block

	p.server.sugar.Infow("got append entries response", "peer", p.id, "request", request)

	go func() {
		defer p.server.sugar.Infow("handled append entries response", "peer", p.id, "request", request)
		p.server.send(response)
	}()
}

func (p *peer) StartHeartbeat() {
	p.server.sugar.Infow("peer heartbeat starting", "peer", p.id)

	p.server.routineGroup.Add(1)
	go func() {
		defer p.server.routineGroup.Done()
		p.Heartbeat()
	}()
}

func (p *peer) StopHeartbeat(flush bool) {
	p.server.sugar.Infow("peer heartbeat stopping", "peer", p.id)
	p.stopChannel <- flush
	p.server.sugar.Infow("peer heartbeat stopped", "peer", p.id)
}

func (p *peer) Flush() {
	p.server.sugar.Infow("flushing peer", "peer", p.id)
	p.server.sugar.Infow("finished flushing peer", "peer", p.id)

	// TODO prevlogindex and prevlogterm

	p.SendAppendEntriesRequest(&pb.AppendEntriesRequest{
		Term:         uint64(p.server.raftState.currentTerm),
		LeaderId:     uint64(p.server.id),
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries:      make([][]byte, 0),
		LeaderCommit: uint64(p.server.raftState.commitIndex),
	})
}

// Listens to the heartbeat timeout and flushes an AppendEntries RPC
func (p *peer) Heartbeat() {
	ticker := time.Tick(p.heartbeatInterval)

	for {
		select {
		case flush := <-p.stopChannel:
			p.server.sugar.Infow("heartbeat stopped", "peer", p.id, "flush", flush)
			if flush {
				p.Flush()
				return
			}
		case <-ticker:
			p.server.sugar.Infow("heartbeat timeout elapsed", "peer", p.id)
			p.Flush()
		}
	}
}
