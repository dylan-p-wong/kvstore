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
	ResponseChannel chan RPCResponse
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
		if s.raftState.state == FOLLOWER {
			s.followerLoop()
		} else if s.raftState.state == CANDIDATE {
			s.candidateLoop()
		} else if s.raftState.state == LEADER {
			s.leaderLoop()
		}
	}
}

func (s *Server) followerLoop() {
	s.sugar.Infow("starting follower event loop")

	// follower event loop
	s.sugar.Infow("waiting for events", "state", "FOLLOWER")
	for s.raftState.state == FOLLOWER {
		// should be random between an interval
		timeoutChannel := time.After(s.electionTimeout)

		select {
		case <-s.stopped:
			return
		case event := <-s.events:
			s.sugar.Infow("event recieved", "state", "FOLLOWER")
			switch event.Command.(type) {
			case *pb.AppendEntriesRequest:
				s.sugar.Infow("recieved append entries request", "state", "FOLLOWER")
				event.ResponseChannel <- s.processAppendEntriesRequest(event.Command.(*pb.AppendEntriesRequest))
			case *pb.RequestVoteRequest:
				s.sugar.Infow("recieved request vote request", "state", "FOLLOWER")
				event.ResponseChannel <- s.processRequestVoteRequest(event.Command.(*pb.RequestVoteRequest))
			}
		case <-timeoutChannel:
			s.sugar.Infow("follower timeout")
			s.raftState.state = CANDIDATE
		}
	}
}

func (s *Server) candidateLoop() {
	s.sugar.Infow("starting candidate event loop")

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
						LastLogIndex: 0, // TODO
						LastLogTerm:  0, // TODO
					}, requestVoteResponseChannel)
				}(p)
			}

			// should be random between an interval
			timeoutChannel = time.After(s.electionTimeout)
			doVote = false
		}

		if votesGranted == (len(s.peers)+1)/2+1 {
			s.sugar.Infow("candidate promoted to leader", "votes_granted", votesGranted)
			s.raftState.state = LEADER
			return
		}

		// candidate event loop
		s.sugar.Infow("waiting for events", "state", "CANDIDATE")
		select {
		case <-s.stopped:
			s.sugar.Infow("candidate stopped")
			s.raftState.state = STOPPED
			return
		case response := <-requestVoteResponseChannel:
			success := s.processRequestVoteResponse(response)
			s.sugar.Infow("recieved request vote response", "state", "CANDIDATE")
			if success {
				votesGranted++
				s.sugar.Infow("recieved vote", "votes_granted", votesGranted)
			}
		case event := <-s.events:
			s.sugar.Infow("event recieved", "state", "CANDIDATE")
			switch event.Command.(type) {
			case *pb.AppendEntriesRequest:
				s.sugar.Infow("recieved append entries request", "state", "CANDIDATE")
				event.ResponseChannel <- s.processAppendEntriesRequest(event.Command.(*pb.AppendEntriesRequest))
			case *pb.RequestVoteRequest:
				s.sugar.Infow("recieved request vote request", "state", "CANDIDATE")
				event.ResponseChannel <- s.processRequestVoteRequest(event.Command.(*pb.RequestVoteRequest))
			}
		case <-timeoutChannel:
			s.sugar.Infow("candidate timeout")
			doVote = true
		}
	}
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

	// leader event loop
	s.sugar.Infow("waiting for events", "state", "LEADER")
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
			s.sugar.Infow("event recieved", "state", "LEADER")
			switch event.Command.(type) {
			case *pb.PutRequest:
				s.sugar.Infow("recieved PUT request", "state", "LEADER")
				s.processPutRequest(event.Command.(*pb.PutRequest), event.ResponseChannel)
				continue
			case *pb.AppendEntriesRequest:
				s.sugar.Infow("recieved append entries request", "state", "LEADER")
				event.ResponseChannel <- s.processAppendEntriesRequest(event.Command.(*pb.AppendEntriesRequest))
			case *pb.RequestVoteRequest:
				s.sugar.Infow("recieved request vote request", "state", "LEADER")
				event.ResponseChannel <- s.processRequestVoteRequest(event.Command.(*pb.RequestVoteRequest))
			case *pb.AppendEntriesResponse:
				s.sugar.Infow("recieved append entries response", "state", "LEADER")
				s.processAppendEntriesResponse(event.Command.(*pb.AppendEntriesResponse))
				// Signal to a send we are done
				event.ResponseChannel <- RPCResponse{}
			}
		}
	}
}

func (s *Server) handleAllServerRequestResponseRules(term int) {
	// see rules for all servers in raft paper
	if term > s.raftState.currentTerm {
		if s.raftState.state == LEADER {
			for _, p := range s.peers {
				p.StopHeartbeat(false)
			}
		}
		s.raftState.state = FOLLOWER
		s.raftState.currentTerm = int(term)		
	}
}


func (s *Server) processRequestVoteRequest(request *pb.RequestVoteRequest) RPCResponse {
	s.sugar.Infow("processing request vote request", "request", request)
	s.handleAllServerRequestResponseRules(int(request.Term))

	var response RPCResponse
	response.Error = nil


	// reply false if term < currentTerm
	if request.Term < uint64(s.raftState.currentTerm) {
		response.Response = &pb.RequestVoteResponse{
			Term:        uint64(s.raftState.currentTerm),
			VoteGranted: false,
		}
		return response
	}

	// if votedFor is null or candidateId AND TODO(candidate log is at least as up to date as reciever log) grant vote
	if s.raftState.votedFor == -1 || s.raftState.votedFor != s.id {
		s.raftState.votedFor = int(request.CandidateId)

		response.Response = &pb.RequestVoteResponse{
			Term:        uint64(s.raftState.currentTerm),
			VoteGranted: true,
		}
		return response
	}
	
	response.Response = &pb.RequestVoteResponse{
		Term:        uint64(s.raftState.currentTerm),
		VoteGranted: false,
	}
	return response
}

func (s *Server) processPutRequest(request *pb.PutRequest, responseChannel chan<- RPCResponse) {
	s.sugar.Infow("processing PUT request", "request", request)

	nextIndex := s.getCurrentLogIndex() + 1

	entry := newLogEntry(s.raftState.currentTerm, nextIndex, string(request.Key), string(request.Value))

	err := s.appendLogEntry(entry)

	if err != nil {
		s.sugar.Infow("error appending entry to log", err)
		responseChannel <- RPCResponse{
			Error: err,
		}
	}

	if len(s.peers) == 0 {
		s.raftState.commitIndex = s.getCurrentLogIndex()
	}
}

func (s *Server) processRequestVoteResponse(response *pb.RequestVoteResponse) bool {
	s.sugar.Infow("processing request vote response", "response", response)
	s.handleAllServerRequestResponseRules(int(response.Term))

	if response.VoteGranted && response.Term == uint64(s.raftState.currentTerm) {
		return true
	}

	return false
}


func (s *Server) processAppendEntriesRequest(request *pb.AppendEntriesRequest) RPCResponse {
	s.sugar.Infow("processing append entries request", "request", request)
	s.handleAllServerRequestResponseRules(int(request.Term))

	var response RPCResponse
	response.Error = nil

	if s.raftState.currentTerm <= int(request.Term) {
		if s.raftState.state == CANDIDATE {
			s.sugar.Infow("stepping down to follower")
			s.raftState.state = FOLLOWER
		}
	} else {
		response.Response = &pb.AppendEntriesResponse{
			Term:    uint64(s.raftState.currentTerm),
			Success: false,
		}
		return response
	}

	response.Response = &pb.AppendEntriesResponse{
		Term:    uint64(s.raftState.currentTerm),
		Success: true,
	}

	return response
}

func (s *Server) processAppendEntriesResponse(response *pb.AppendEntriesResponse) {
	s.sugar.Infow("processing append entries response", "response", response)
	s.handleAllServerRequestResponseRules(int(response.Term))
}
