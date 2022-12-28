package service

import (
	"errors"
	"sort"
	"time"

	pb "github.com/dylan-p-wong/kvstore/api"
)

type EventResponse struct {
	Response interface{}
	Error    error
}

type EventRequest struct {
	Command         interface{}
	ResponseChannel chan EventResponse
}

// sends to event loop
func (s *server) send(command interface{}) (interface{}, error) {
	channel := make(chan EventResponse, 1)

	event := EventRequest{
		Command:         command,
		ResponseChannel: channel,
	}

	s.events <- event

	// wait until we get a response on the channel
	response := <-channel

	if response.Error != nil {
		return nil, response.Error
	}

	return response.Response, nil
}

func (s *server) loop() {
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

func (s *server) followerLoop() {
	s.sugar.Infow("starting follower event loop")

	// follower event loop
	s.sugar.Infow("waiting for events", "state", "FOLLOWER")
	for s.raftState.state == FOLLOWER {
		// should be random between an interval
		timeoutChannel := time.After(s.getElectionTimeout())

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
			s.raftState.leader = -1
		}
	}
}

func (s *server) candidateLoop() {
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
			err := s.persistCurrentTerm()
			if err != nil {
				panic(err) // TODO: is there something else we should do?
			}

			// votes for itself
			s.raftState.votedFor = s.id
			votesGranted = 1

			// persist votedFor
			err = s.persistVotedFor()
			if err != nil {
				panic(err) // TODO: is there something else we should do?
			}

			requestVoteResponseChannel = make(chan *pb.RequestVoteResponse, len(s.peers))
			for _, p := range s.peers {
				s.routineGroup.Add(1)
				go func(p *peer) {
					defer s.routineGroup.Done()
					p.sendVoteRequest(&pb.RequestVoteRequest{
						Term:         uint64(s.raftState.currentTerm),
						CandidateId:  uint64(s.id),
						LastLogIndex: uint64(s.GetLastLogIndex()),
						LastLogTerm:  uint64(s.GetLastLogTerm()),
					}, requestVoteResponseChannel)
				}(p)
			}

			timeoutChannel = time.After(s.getElectionTimeout())
			doVote = false
		}

		if votesGranted == (len(s.peers)+1)/2+1 {
			s.sugar.Infow("candidate promoted to leader", "votes_granted", votesGranted)
			s.raftState.state = LEADER
			s.raftState.leader = s.id
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

func (s *server) leaderLoop() {
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
		p.startHeartbeat()
	}

	// send inital requests to claim
	s.routineGroup.Add(1)
	go func() {
		defer s.routineGroup.Done()
		for _, p := range s.peers {
			go p.sendAppendEntriesRequest(&pb.AppendEntriesRequest{
				Term:         uint64(s.raftState.currentTerm),
				LeaderId:     uint64(s.id),
				PrevLogIndex: uint64(p.getPrevLogIndex(s.raftState.nextIndex[p.id])),
				PrevLogTerm:  uint64(p.getPrevLogTerm(s.raftState.nextIndex[p.id])),
				Entries:      make([]*pb.LogEntry, 0), // empty to gain leadership
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
				p.stopHeartbeat(false)
			}
			s.raftState.state = STOPPED
			return
		case event := <-s.events:
			s.sugar.Infow("event recieved", "state", "LEADER")
			switch event.Command.(type) {
			case *pb.PutRequest:
				s.sugar.Infow("recieved PUT request", "state", "LEADER")
				s.processPutRequest(event.Command.(*pb.PutRequest), event.ResponseChannel)
			case *pb.AppendEntriesRequest:
				s.sugar.Infow("recieved append entries request", "state", "LEADER")
				event.ResponseChannel <- s.processAppendEntriesRequest(event.Command.(*pb.AppendEntriesRequest))
			case *pb.RequestVoteRequest:
				s.sugar.Infow("recieved request vote request", "state", "LEADER")
				event.ResponseChannel <- s.processRequestVoteRequest(event.Command.(*pb.RequestVoteRequest))
			case *pb.AppendEntriesResponse:
				s.sugar.Infow("recieved append entries response", "state", "LEADER")
				event.ResponseChannel <- s.processAppendEntriesResponse(event.Command.(*pb.AppendEntriesResponse))
			}
		}
	}
}

func (s *server) processPutRequest(request *pb.PutRequest, responseChannel chan EventResponse) {
	s.sugar.Infow("processing PUT request", "request", request)

	// encode put request into command
	command, err := encodeCommand(string(request.Key), string(request.Value))
	if err != nil {
		responseChannel <- EventResponse{
			Error: err,
		}
		return
	}

	nextIndex := s.GetLastLogIndex() + 1
	entry := newLogEntry(s.raftState.currentTerm, nextIndex, command, responseChannel)

	err = s.appendLogEntry(entry)

	if err != nil {
		s.sugar.Infow("error appending entry to log", err)
		responseChannel <- EventResponse{
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

func (s *server) handleAllServerRequestResponseRules(term int, serverId int) {
	// see rules for all servers in raft paper
	if term > s.raftState.currentTerm {
		if s.raftState.state == LEADER {
			for _, p := range s.peers {
				p.stopHeartbeat(false)
			}
		}
		s.raftState.state = FOLLOWER
		s.raftState.leader = serverId

		s.raftState.votedFor = -1
		s.persistVotedFor()

		s.raftState.currentTerm = int(term)
		s.persistCurrentTerm()
	}
}

func (s *server) candidateUpToDate(candidateLastLogTerm uint64, candidateLastLogIndex uint64) bool {
	if uint64(s.GetLastLogTerm()) == candidateLastLogTerm {
		return uint64(s.GetLastLogIndex()) <= candidateLastLogTerm
	}
	return uint64(s.GetLastLogTerm()) <= candidateLastLogTerm
}

func (s *server) processRequestVoteRequest(request *pb.RequestVoteRequest) EventResponse {
	s.sugar.Infow("processing request vote request", "request", request)
	s.handleAllServerRequestResponseRules(int(request.Term), int(request.CandidateId))

	var response EventResponse
	response.Error = nil

	// reply false if term < currentTerm
	if request.Term < uint64(s.raftState.currentTerm) {
		response.Response = &pb.RequestVoteResponse{
			Term:        uint64(s.raftState.currentTerm),
			VoteGranted: false,
		}
		return response
	}

	// if votedFor is null or candidateId AND candidate log is at least as up to date as reciever log grant vote
	if s.raftState.votedFor == -1 || s.raftState.votedFor == int(request.CandidateId) && s.candidateUpToDate(request.LastLogTerm, request.LastLogIndex) {
		// set voted for to candidate
		s.raftState.votedFor = int(request.CandidateId)
		// persist votedFor
		err := s.persistVotedFor()
		if err != nil {
			panic(err) // TODO: is there something else we should do?
		}

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

func (s *server) processRequestVoteResponse(response *pb.RequestVoteResponse) bool {
	s.sugar.Infow("processing request vote response", "response", response)
	s.handleAllServerRequestResponseRules(int(response.Term), -1)

	if response.VoteGranted && response.Term == uint64(s.raftState.currentTerm) {
		return true
	}

	return false
}

func (s *server) processAppendEntriesRequest(request *pb.AppendEntriesRequest) EventResponse {
	// we assume entries are sorted by index

	s.sugar.Infow("processing append entries request", "request", request)
	s.handleAllServerRequestResponseRules(int(request.Term), int(request.LeaderId))

	var response EventResponse
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
			newLe := newLogEntry(int(rle.Term), int(rle.Index), rle.CommandName, nil)
			s.raftState.log = append(s.raftState.log, &newLe)
		}
	}

	// 5. if leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if int(request.LeaderCommit) > s.raftState.commitIndex {
		if int(request.LeaderCommit) < len(s.raftState.log) {
			s.raftState.commitIndex = int(request.LeaderCommit)
		} else {
			s.raftState.commitIndex = len(s.raftState.log)
		}
	}

	// see all servers bullet 2
	for s.raftState.lastApplied < s.raftState.commitIndex {
		// sync s.raftState.log[lastApplied+1-1] to disk
		err := s.persistLogEntry(s.raftState.log[s.raftState.lastApplied])
		if err == nil {
			s.raftState.lastApplied++
		} else {
			panic(err) // TODO: should we do something else?
		}
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

var ErrNotLeader = errors.New("not leader")
var ErrUnsuccessfulAppendEntries = errors.New("unsuccessful append entries request")

func (s *server) processAppendEntriesResponse(response *pb.AppendEntriesResponse) EventResponse {
	s.sugar.Infow("processing append entries response", "response", response)

	s.handleAllServerRequestResponseRules(int(response.Term), int(response.ServerId))
	// if handleAllServerRequestResponseRules changed state
	if s.raftState.state != LEADER {
		return EventResponse{Error: ErrNotLeader}
	}

	// see leaders bullet 5
	// if AppendEntries fails because of log inconsistency: decrement nextIndex and retry
	// retry will be done on the next peer heartbeat
	if response.Success == false {
		s.raftState.nextIndex[int(response.ServerId)]--
		return EventResponse{Error: ErrUnsuccessfulAppendEntries}
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

	for s.raftState.commitIndex < commitedIndex {
		currentCommitedIndex := s.raftState.commitIndex
		if s.raftState.log[currentCommitedIndex+1-1].term == s.raftState.currentTerm {
			// sync log entry to disk
			err := s.persistLogEntry(s.raftState.log[currentCommitedIndex+1-1])

			// we had an error setting in storage
			if err != nil {
				panic(err)
			}

			s.raftState.lastApplied = currentCommitedIndex + 1

			// reply to waiting channel that the command has been replicated
			if s.raftState.log[currentCommitedIndex+1-1].responseChannel != nil { // will this case ever happen
				s.raftState.log[currentCommitedIndex+1-1].responseChannel <- EventResponse{
					Error: nil,
				}
			}
		}
		s.raftState.commitIndex++
	}

	return EventResponse{Error: nil}
}
