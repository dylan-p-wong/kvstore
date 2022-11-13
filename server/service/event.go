package service

import (
	"time"

	pb "github.com/dylan-p-wong/kvstore/api"
)

type RPCResponse struct {
	Response interface{}
	Error    error
}

type RPCRequest struct {
	Command         interface{}
	ResponseChannel chan<- RPCResponse
}

// Sends to event loop
func (s *Server) send(command interface{}) (interface{}, error) {
	channel := make(chan RPCResponse, 1)

	rpc := RPCRequest{
		Command:         command,
		ResponseChannel: channel,
	}

	s.events <- rpc

	response := <-channel

	if response.Error != nil {
		return nil, response.Error
	}

	return response.Response, nil
}

func (s *Server) loop() {
	for s.raftState.state != STOPPED {
		if s.raftState.state == LEADER {
			s.leaderLoop()
		} else if s.raftState.state == FOLLOWER {
			s.followerLoop()
		} else if s.raftState.state == CANDIDATE {
			s.candidateLoop()
		}
	}
}

func (s *Server) followerLoop() {
	s.sugar.Infow("starting follower event loop")

	// should be random between an interval
	timeoutChannel := time.After(s.electionTimeout)

	for s.raftState.state == FOLLOWER {
		select {
		case <-s.stopped:
			return
		case event := <-s.events:
			switch event.Command.(type) {
			case *pb.AppendEntriesRequest:
				s.processAppendEntriesRequest(event.Command.(*pb.AppendEntriesRequest))
			case *pb.RequestVoteRequest:
				s.processRequestVoteRequest(event.Command.(*pb.RequestVoteRequest))
			}
		case <-timeoutChannel:
			s.sugar.Infow("follower timeout")
			s.raftState.state = CANDIDATE
		}
	}
}

func (s *Server) candidateLoop() {
	s.sugar.Infow("starting candidate event loop")

	s.leader = -1

	doVote := true
	votesGranted := 0

	var timeoutChannel <-chan time.Time
	var requestVoteResponseChannel chan *pb.RequestVoteResponse

	for s.raftState.state == CANDIDATE {

		if doVote {
			s.sugar.Infow("candidate sending votes")

			// Increment current term
			s.raftState.currentTerm++

			// votes for itself
			s.raftState.votedFor = s.id
			votesGranted = 1

			requestVoteResponseChannel = make(chan *pb.RequestVoteResponse, len(s.peers))
			for _, p := range s.peers {
				s.routineGroup.Add(1)
				go func(p *peer) {
					defer s.routineGroup.Done()
					p.SendVoteRequest(&pb.RequestVoteRequest{
						Term:         uint64(s.raftState.currentTerm),
						CandidateId:  uint64(s.id),
						LastLogIndex: 0, // TODOD
						LastLogTerm:  0,
					}, requestVoteResponseChannel)
				}(p)
			}

			// should be random between an interval
			timeoutChannel = time.After(s.electionTimeout)
			doVote = false
		}

		if votesGranted == (len(s.peers)+1)/2 {
			s.sugar.Infow("candidate promoted to leader")
			s.raftState.state = LEADER
			return
		}

		// candidate event loop
		select {
		case <-s.stopped:
			s.sugar.Infow("candidate stopped")
			s.raftState.state = STOPPED
			return
		case response := <-requestVoteResponseChannel:
			success := s.processRequestVoteResponse(response)
			if success {
				votesGranted++
			}
		case event := <-s.events:
			switch event.Command.(type) {
			case *pb.AppendEntriesRequest:
				s.processAppendEntriesRequest(event.Command.(*pb.AppendEntriesRequest))
			case *pb.RequestVoteRequest:
				s.processRequestVoteRequest(event.Command.(*pb.RequestVoteRequest))
			}
		case <-timeoutChannel:
			s.sugar.Infow("candidate timeout")
			doVote = true
		}
	}
}

func (s *Server) processRequestVoteResponse(response *pb.RequestVoteResponse) bool {
	if response.VoteGranted && response.Term == uint64(s.raftState.currentTerm) {
		return true
	}

	if response.Term > uint64(s.raftState.currentTerm) {
		s.updateCurrentTerm(int(response.Term), -1)
	}

	return false
}

func (s *Server) leaderLoop() {
	s.sugar.Infow("starting leader event loop")

	s.sugar.Infow("starting peer heatbeats", "peers", s.peers)
	for _, p := range s.peers {
		p.StartHeartbeat()
	}

	// send inital requests to claim
	s.routineGroup.Add(1)
	go func() {
		defer s.routineGroup.Done()
		s.send(&pb.PutRequest{})
	}()

	for s.raftState.state == LEADER {
		select {
		case <-s.stopped:
			s.sugar.Infow("stopping peer heatbeats", "peers", len(s.peers))
			for _, p := range s.peers {
				p.StopHeartbeat(false)
			}
			s.raftState.state = STOPPED
			return
		case event := <-s.events:
			switch event.Command.(type) {
			case *pb.PutRequest:
				s.processPutRequest(event.Command.(*pb.PutRequest), event.ResponseChannel)
				continue
			case *pb.AppendEntriesRequest:
				s.processAppendEntriesRequest(event.Command.(*pb.AppendEntriesRequest))
				event.ResponseChannel <- RPCResponse{
					Response: &pb.AppendEntriesResponse{
						Term:    event.Command.(*pb.AppendEntriesResponse).Term,
						Success: true,
					},
					Error: nil,
				}
			case *pb.RequestVoteRequest:
				s.processRequestVoteRequest(event.Command.(*pb.RequestVoteRequest))
				event.ResponseChannel <- RPCResponse{
					Response: &pb.RequestVoteResponse{
						Term:        event.Command.(*pb.RequestVoteRequest).Term,
						VoteGranted: true,
					},
					Error: nil,
				}
			case *pb.AppendEntriesResponse:
				s.processAppendEntriesResponse(event.Command.(*pb.AppendEntriesResponse))
			}
		}
	}
}

func (s *Server) processAppendEntriesRequest(request *pb.AppendEntriesRequest) RPCResponse {
	var response RPCResponse

	// 1.
	if request.Term < uint64(s.raftState.currentTerm) {
		response.Response = &pb.AppendEntriesResponse{
			Term:    uint64(s.raftState.currentTerm),
			Success: false,
		}
		return response
	}

	if request.Term == uint64(s.raftState.currentTerm) {
		if s.raftState.state == CANDIDATE {
			s.raftState.state = FOLLOWER
		}

		s.leader = int(request.LeaderId)
	} else {
		s.updateCurrentTerm(int(request.Term), int(request.LeaderId))
	}

	// 2.
	found := request.PrevLogIndex == 0
	for _, logEntry := range s.raftState.log {
		if logEntry.index == int(request.PrevLogIndex) && logEntry.term == int(request.PrevLogTerm) {
			found = true
		}
	}

	if !found {
		response.Response = &pb.AppendEntriesResponse{
			Term:    uint64(s.raftState.currentTerm),
			Success: false,
		}
		return response
	}

	// TODO

	// 5.
	if request.LeaderCommit > uint64(s.raftState.commitIndex) {
		if int(request.LeaderCommit) < len(s.raftState.log)-1 {
			s.raftState.commitIndex = int(request.LeaderCommit)
		} else {
			s.raftState.commitIndex = len(s.raftState.log)
		}
	}

	response.Response = &pb.AppendEntriesResponse{
		Term:    uint64(s.raftState.currentTerm),
		Success: true,
	}

	return response
}

func (s *Server) processAppendEntriesResponse(response *pb.AppendEntriesResponse) {

}

// called when we find a higher term from a peer
func (s *Server) updateCurrentTerm(term int, leaderId int) {

	if s.raftState.state == LEADER {
		for _, p := range s.peers {
			p.StopHeartbeat(false)
		}
	}

	s.raftState.state = FOLLOWER

	s.raftState.currentTerm = term
	s.leader = leaderId
	s.raftState.votedFor = -1
}

func (s *Server) processRequestVoteRequest(request *pb.RequestVoteRequest) RPCResponse {
	var response RPCResponse

	if request.Term < uint64(s.raftState.currentTerm) {
		response.Response = &pb.RequestVoteResponse{
			Term:        uint64(s.raftState.currentTerm),
			VoteGranted: false,
		}
		return response
	}

	if request.Term > uint64(s.raftState.currentTerm) {
		s.updateCurrentTerm(int(request.Term), int(request.CandidateId))
	} else if s.raftState.votedFor != -1 && s.raftState.votedFor != s.id {
		response.Response = &pb.RequestVoteResponse{
			Term:        uint64(s.raftState.currentTerm),
			VoteGranted: false,
		}
		return response
	}

	if !s.candidateLogUpToDate(request) {
		response.Response = &pb.RequestVoteResponse{
			Term:        uint64(s.raftState.currentTerm),
			VoteGranted: false,
		}
		return response
	}

	response.Response = &pb.RequestVoteResponse{
		Term:        uint64(s.raftState.currentTerm),
		VoteGranted: true,
	}
	return response
}

func (s *Server) candidateLogUpToDate(request *pb.RequestVoteRequest) bool {
	return true
}

func (s *Server) processPutRequest(request *pb.PutRequest, responseChannel chan<- RPCResponse) {
	nextIndex := s.getCurrentLogIndex() + 1

	entry := newLogEntry(s.raftState.currentTerm, nextIndex, string(request.Key), string(request.Value))

	err := s.appendLogEntry(entry)

	if err != nil {
		responseChannel <- RPCResponse{
			Error: err,
		}
	}

	if len(s.peers) == 0 {
		s.raftState.commitIndex = s.getCurrentLogIndex()
	}
}
