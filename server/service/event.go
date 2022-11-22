package service

import (
	"errors"
	"sort"
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

// sends to event loop
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

	// see voliatile state on leaders
	// we initialize nextIndex to leader last log index + 1
	// we initialize matchIndex to 0
	for _, p := range s.peers {
		s.raftState.nextIndex[p.id] = s.GetLastLogIndex() + 1
		s.raftState.matchIndex[p.id] = 0
	}
	s.raftState.nextIndex[s.id] = s.GetLastLogIndex() + 1
	s.raftState.matchIndex[s.id] = 0

	s.sugar.Infow("starting peer heatbeats", "peers", s.peers)
	for _, p := range s.peers {
		p.StartHeartbeat()
	}

	// send inital requests to claim
	s.routineGroup.Add(1)
	go func() {
		defer s.routineGroup.Done()
		for _, p := range s.peers {
			go p.SendAppendEntriesRequest(&pb.AppendEntriesRequest{
				Term:         uint64(s.raftState.currentTerm),
				LeaderId:     uint64(s.id),
				PrevLogIndex: uint64(p.GetPrevLogIndex(s.raftState.nextIndex[p.id])),
				PrevLogTerm:  uint64(p.GetPrevLogTerm(s.raftState.nextIndex[p.id])),
				Entries:      make([]*pb.LogEntry, 0),
				LeaderCommit: uint64(s.raftState.commitIndex),
			})
		}
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

func (s *Server) processPutRequest(request *pb.PutRequest, responseChannel chan RPCResponse) {
	s.sugar.Infow("processing PUT request", "request", request)

	nextIndex := s.GetLastLogIndex() + 1

	entry := newLogEntry(s.raftState.currentTerm, nextIndex, string(request.Key)+":"+string(request.Value), responseChannel)

	err := s.appendLogEntry(entry)

	s.sugar.Infow("appended log entry to log", "index", entry.index, "term", entry.term, "command", entry.command)

	if err != nil {
		s.sugar.Infow("error appending entry to log", err)
		responseChannel <- RPCResponse{
			Error: errors.New("error appending entry to log"),
		}
	}

	// set match index for itself
	s.raftState.matchIndex[s.id] = len(s.raftState.log)
	s.raftState.nextIndex[s.id] = s.raftState.matchIndex[s.id] + 1

	if len(s.peers) == 0 {
		s.raftState.commitIndex = s.GetLastLogIndex()
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
	// we assume entries are sorted by index

	s.sugar.Infow("processing append entries request", "request", request)
	s.handleAllServerRequestResponseRules(int(request.Term))

	var response RPCResponse
	response.Error = nil

	// 1. reply false if term < currentTerm
	if int(request.Term) < s.raftState.currentTerm {
		s.sugar.Infow("append entries failed", "step", 1)
		response.Response = &pb.AppendEntriesResponse{
			Term:         uint64(s.raftState.currentTerm),
			Success:      false,
			ServerId:     uint64(s.id),
			PrevLogIndex: request.PrevLogIndex,
			Entries:      request.Entries,
		}
		return response
	}

	// 2. reply false if log does not contain an entry at prevLogIndex whose term matches prevLogTerm
	if len(s.raftState.log) < int(request.PrevLogIndex) || (request.PrevLogIndex > 0 && s.raftState.log[request.PrevLogIndex-1].term != int(request.PrevLogTerm)) {
		// fails because of log inconsistency
		s.sugar.Infow("append entries failed", "step", 2)
		response.Response = &pb.AppendEntriesResponse{
			Term:         uint64(s.raftState.currentTerm),
			Success:      false,
			ServerId:     uint64(s.id),
			PrevLogIndex: request.PrevLogIndex,
			Entries:      request.Entries,
		}
		return response
	}

	// 3. if an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it
	for _, rle := range request.Entries {
		if len(s.raftState.log) >= int(rle.Index) && s.raftState.log[rle.Index-1].term != int(rle.Term) {
			s.raftState.log = s.raftState.log[:(rle.Index - 1)]
			break
		}
	}

	// 4. append any new entries not already in log
	// TODO: revisit and make faster
	for _, rle := range request.Entries {
		duplicate := false
		for _, le := range s.raftState.log {
			if int(rle.Index) == le.index {
				duplicate = true
			}
		}

		if !duplicate {
			newLe := newLogEntry(int(rle.Term), int(rle.Index), rle.CommandName, make(chan RPCResponse))
			s.raftState.log = append(s.raftState.log, &newLe)
		}
	}

	// 5. if leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if request.LeaderCommit > uint64(s.raftState.commitIndex) {
		if int(request.LeaderCommit) < len(s.raftState.log) {
			s.raftState.commitIndex = int(request.LeaderCommit)
		} else {
			s.raftState.commitIndex = len(s.raftState.log)
		}
	}

	// see all servers bullet 2
	for s.raftState.lastApplied < s.raftState.commitIndex {
		// TODO: sync s.raftState.log[lastApplied+1-1] to disk
		s.raftState.lastApplied++
	}

	response.Response = &pb.AppendEntriesResponse{
		Term:         uint64(s.raftState.currentTerm),
		Success:      true,
		ServerId:     uint64(s.id),
		PrevLogIndex: request.PrevLogIndex,
		Entries:      request.Entries,
	}

	return response
}

func (s *Server) processAppendEntriesResponse(response *pb.AppendEntriesResponse) {
	s.sugar.Infow("processing append entries response", "response", response)
	s.handleAllServerRequestResponseRules(int(response.Term))
	// if handleAllServerRequestResponseRules changed state
	if s.raftState.state != LEADER {
		return
	}

	// see leaders bullet 5
	// if AppendEntries fails because of log inconsistency: decrement nextIndex and retry
	// retry will be done on the next peer heartbeat
	if response.Success == false {
		s.raftState.nextIndex[int(response.ServerId)]--
		return
	}

	// see leaders bullet 4
	// if successful: update nextIndex and matchIndex for follower
	s.raftState.matchIndex[int(response.ServerId)] = int(response.PrevLogIndex) + len(response.Entries)
	s.raftState.nextIndex[int(response.ServerId)] = s.raftState.matchIndex[int(response.ServerId)] + 1

	// see leaders bullet 6
	// if there exists an N such that N > commitIndex, a majority of matchIndex[i] >= N, and log[N].term == currentTerm: set commitIndex = N
	// see leaders bullet 2
	// apply entries to state machine then update commited index
	// we must then send a response on the log entry response channel
	matchIndexes := make([]int, 0)
	for _, v := range s.raftState.matchIndex {
		matchIndexes = append(matchIndexes, v)
	}
	sort.Ints(matchIndexes)

	commitedIndex := matchIndexes[(len(s.peers)+1)/2+1-1]

	if commitedIndex > s.raftState.commitIndex {
		if s.raftState.log[commitedIndex-1].term == s.raftState.currentTerm {
			s.raftState.commitIndex = matchIndexes[(len(s.peers)+1)/2+1-1]
			// TODO: sync log entry to disk
			s.raftState.lastApplied = s.raftState.commitIndex
			// reply to waiting channel that the command has been replicated
			s.raftState.log[commitedIndex-1].responseChannel <- RPCResponse{
				Error: nil,
			}
		}
	}
}
