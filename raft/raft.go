package raft

import (
	"errors"
	"github.com/golang/glog"
	"math/rand"
	"net/rpc"
	"sort"
	"sync"
	"time"
)

const (
	MinWaitInMillis = 4000
	MaxWaitInMillis = 8000
	HeartbeatInSecs = 1
)

type Command struct {
	Variable string
	Value    uint
}

type Entry struct {
	Command Command
	Term    int
}

type State int

const (
	Follower State = iota
	Candidate
	Leader
)

type Peer struct {
	Id     int
	Port   string
	Client *rpc.Client
}

type Raft struct {
	// should be ideally stored on disk
	currentTerm int
	votedFor    *int
	log         []Entry

	// volatile state
	commitIndex           int
	lastApplied           int
	lastUpdatedFromLeader time.Time
	randomElectionTimeout int
	currentState          State
	id                    int
	peers                 []Peer
	leaderId int

	// leader
	nextIndex         []int
	matchIndex        []int
	lastHeartbeatSent time.Time

	// mutex
	mu sync.Mutex

	// to induce node failure
	IsAlive bool
}

type ExecuteReply struct {
	Index int
	IsReplicated bool
}

func (r *Raft) Execute(command Command, reply *ExecuteReply) error {
	r.mu.Lock()
	if r.currentState != Leader {
		return errors.New("not a leader! ")
	}

	// append to local log
	r.log = append(r.log, Entry{
		Command: command,
		Term:    r.currentTerm,
	})
	r.matchIndex[r.id] += 1

	r.mu.Unlock()
	commitIndex, isReplicated := r.replicateCommandToFollowers()
	reply.Index = commitIndex
	reply.IsReplicated = isReplicated
	return nil
}

// election timeout goroutine
func (r *Raft) electionTimeout() {
	for {
		r.mu.Lock()
		if r.currentState != Leader && r.IsAlive {
			glog.V(1).Infof("raft: %v, currentTerm: %v, lastUpdatedDiff: %v, randomElectionTimeoutInSecs: %v\n", r.id, r.currentTerm, time.Now().Sub(r.lastUpdatedFromLeader).Milliseconds(), r.randomElectionTimeout)
			for time.Now().Sub(r.lastUpdatedFromLeader).Milliseconds() < int64(r.randomElectionTimeout) {
				r.mu.Unlock()
				time.Sleep(1 * time.Second)
				r.mu.Lock()
			}

			if r.currentState == Leader {
				r.mu.Unlock()
				continue
			}

			r.lastUpdatedFromLeader = time.Now()
			r.randomElectionTimeout = getRandomElectionTimeoutInMillis()
			r.becomeCandidate()
			r.mu.Unlock()
			go r.conductElection()
		} else {
			r.mu.Unlock()
		}
	}
}

func (r *Raft) conductElection() {
	totalVotes := 0
	totalReceived := 0
	r.mu.Lock()
	if r.currentState == Follower || !r.IsAlive {
		r.mu.Unlock()
		return
	}
	r.currentTerm += 1
	r.votedFor = &r.id
	totalVotes += 1
	r.lastUpdatedFromLeader = time.Now()
	r.randomElectionTimeout = getRandomElectionTimeoutInMillis()
	currentTermCopy := r.currentTerm
	var lastLogIndex, lastLogTerm int
	if len(r.log) == 0 {
		lastLogIndex = -1
		lastLogTerm = -1
	} else {
		lastLogIndex = len(r.log) - 1
		lastLogTerm = r.log[lastLogIndex].Term
	}

	majority := getMajority(len(r.peers))
	r.mu.Unlock()

	cond := sync.NewCond(&r.mu)

	glog.V(1).Infof("raft: %v, let's get some votes for term: %v\n", r.id, r.currentTerm)

	for _, peer := range r.peers {
		if peer.Id == r.id {
			continue
		}
		go func(p Peer) {
			defer func() {
				totalReceived += 1
				cond.Broadcast()
			}()
			args := RequestVoteArgs{
				Term:         currentTermCopy,
				CandidateId:  r.id,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			}

			reply := &RequestVoteReply{}
			if r.currentState != Candidate && r.IsAlive {
				return
			}
			err := r.requestVoteRPC(p.Id, args, reply)
			if err != nil {
				glog.Errorf("raft: %v, from: %v, RequestVote: args: %+v error: %s\n", r.id, p.Id, args, err.Error())
				return
			}
			glog.V(2).Infof("raft: %v, from: %v, RequestVote: args: %+v; reply: %v\n", r.id, p.Id, args, reply)

			r.mu.Lock()
			defer r.mu.Unlock()

			if r.currentState != Candidate && r.IsAlive {
				return
			}

			if reply.VoteGranted {
				totalVotes += 1
			} else if reply.TermToUpdate > r.currentTerm {
				r.currentTerm = reply.TermToUpdate
				r.becomeFollower()
			}
		}(peer)
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	for totalVotes < majority && totalReceived != len(r.peers) - 1 {
		cond.Wait()
	}

	if r.currentState != Candidate {
		// case: received higher term from one of the appendEntries responses and changed to follower
		return
	}

	if totalVotes >= majority {
		glog.V(2).Infof("raft: %v won election for term: %v \n", r.id, r.currentTerm)
		r.becomeLeader()
	} else {
		r.becomeFollower()
		glog.V(1).Infof("raft: %v suffering from split vote/lost election \n", r.id)
	}
}

func (r *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.IsAlive {
		return errors.New("not alive! ")
	}
	if args.Term < r.currentTerm {
		reply.VoteGranted = false
		reply.TermToUpdate = r.currentTerm
		return nil
	}

	// candidate's log should be at least up-to-date as receiver's log
	var lastLogIndex, lastLogTerm int
	if len(r.log) == 0 {
		lastLogIndex = -1
		lastLogTerm = -1
	} else {
		lastLogIndex = len(r.log)-1
		lastLogTerm = r.log[lastLogIndex].Term
	}

	if (r.votedFor == nil || *r.votedFor == args.CandidateId) && (args.LastLogIndex >= lastLogIndex && args.LastLogTerm >= lastLogTerm) {
		reply.VoteGranted = true
		r.votedFor = &args.CandidateId
		r.lastUpdatedFromLeader = time.Now()
		return nil
	}

	reply.VoteGranted = false
	reply.TermToUpdate = r.currentTerm
	return nil
}

func getRandomElectionTimeoutInMillis() int {
	rand.Seed(time.Now().UnixNano())
	timeout := rand.Intn(MaxWaitInMillis-MinWaitInMillis+1) + MinWaitInMillis
	return timeout
}

// for leader: send heartbeats goroutine
func (r *Raft) sendHeartbeatsAsLeader() {
	for {
		r.mu.Lock()
		if r.currentState == Leader && r.IsAlive {
			//glog.V(1).Infof("raft: %v, log: %v, nextIndex: %v, matchIndex: %v, I'm the leader for term %v\n", r.id, r.log, r.nextIndex, r.matchIndex, r.currentTerm)
			glog.V(1).Infof("raft: %v, log: %v, commitIndex: %v, nextIndex: %v, matchIndex: %v, term %v\n", r.id, r.log, r.commitIndex, r.nextIndex, r.matchIndex, r.currentTerm)
			currentTime := time.Now()
			duration := currentTime.Sub(r.lastHeartbeatSent)
			if duration.Seconds() > float64(HeartbeatInSecs) {
				r.lastHeartbeatSent = time.Now()
				r.mu.Unlock()
				_, _ = r.replicateCommandToFollowers()
				time.Sleep(500 * time.Millisecond)
			} else {
				r.mu.Unlock()
				time.Sleep(500 * time.Millisecond)
			}
		} else {
			r.mu.Unlock()
		}
	}
}

func (r *Raft) replicateCommandToFollowers() (int, bool) {
	r.mu.Lock()
	r.lastHeartbeatSent = time.Now()
	currentTermCopy := r.currentTerm
	commitIndexCopy := r.commitIndex
	r.mu.Unlock()

	majority := getMajority(len(r.peers))
	totalReplications := 1
	totalResponses := 0
	cond := sync.NewCond(&r.mu)

	for _, peer := range r.peers {
		if peer.Id == r.id {
			continue
		}
		go func(p Peer) {
			glog.V(2).Infof("raft: %v, sending heartbeat to %v!\n", r.id, p.Id)
			r.mu.Lock()
			totalLogs := len(r.log)
			var prevLogIndexTerm, prevLogIndex, nextIndex int
			var entries []Entry

			if totalLogs == 0 {
				prevLogIndex = -1
				prevLogIndexTerm = -1
			} else {
				nextIndex = r.nextIndex[p.Id]
				prevLogIndex = nextIndex - 1
				if prevLogIndex == -1 {
					prevLogIndexTerm = -1
				} else {
					prevLogIndexTerm = r.log[prevLogIndex].Term
				}
			}
			entries = make([]Entry, totalLogs-nextIndex)
			newOnes := r.log[nextIndex:]
			copy(entries, newOnes)

			r.mu.Unlock()

			args := AppendEntriesArgs{
				Term:         currentTermCopy,
				LeaderId:     r.id,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogIndexTerm,
				Entries:      entries,
				LeaderCommit: commitIndexCopy,
			}

			reply := &AppendEntriesReply{}
			glog.V(2).Infof("raft: %v, to: %v, calling appendEntriesRPC with args: %+v\n", r.id, p.Id, args)
			err := r.appendEntriesRPC(p.Id, args, reply)
			if err != nil {
				totalResponses += 1
				cond.Broadcast()
				glog.Errorf("AppendEntries: args: %+v error: %s\n", args, err.Error())
				return
			}
			glog.V(2).Infof("raft: %v, to: %v, AppendEntries: args: %+v; reply: %v\n", r.id, p.Id, args, reply)
			r.mu.Lock()
			defer r.mu.Unlock()
			if !reply.Success {
				if reply.TermToUpdate > r.currentTerm {
					r.currentTerm = reply.TermToUpdate
					r.becomeFollower()
				} else {
					// If AppendEntries fails because of log inconsistency:
					//decrement nextIndex and retry
					r.nextIndex[p.Id] -= 1
				}
			} else {
				r.nextIndex[p.Id] = nextIndex + len(entries)
				r.matchIndex[p.Id] = r.nextIndex[p.Id] - 1
				totalReplications += 1
				cond.Broadcast()
			}
		}(peer)
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	for totalReplications < majority && totalResponses != len(r.peers) - 1 {
		cond.Wait()
	}

	if totalReplications >= majority {
		r.updateCommitIndex()
		return r.commitIndex, true
	}

	return -1, false
}

func (r *Raft) updateCommitIndex() {
	if len(r.log) <= 0 {
		return
	}

	temp := make([]int, len(r.matchIndex))
	copy(temp, r.matchIndex)
	sort.Ints(temp)
	majority := getMajority(len(r.peers))
	for i := majority - 1; i >= 0; i-- {
		if temp[i] > r.commitIndex && r.log[temp[i]].Term == r.currentTerm {
			r.commitIndex = temp[i]
			break
		}
	}
}

func (r *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	//glog.V(1).Infof("raft: %v, log: %+v, received AppendEntries args: %+v\n", r.id, r.log, args)

	if !r.IsAlive {
		return errors.New("not alive! ")
	}

	// 1. Reply false if term < currentTerm
	if args.Term < r.currentTerm {
		reply.Success = false
		reply.TermToUpdate = r.currentTerm
		return nil
	} else {
		r.currentTerm = args.Term
		r.becomeFollower()

		// 2. Reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm
		if args.PrevLogIndex == -1 {
			// case: heartbeat when no commands have been issued by client
			// or the first command to replicate
			reply.Success = true
		} else if r.log[args.PrevLogIndex].Term != args.PrevLogTerm {
			reply.Success = false
			//reply.TermToUpdate = r.currentTerm
			return nil
		}

		//existingEntries := r.log[args.PrevLogIndex+1:]
		//newOnes := args.Entries

		// 3. If an existing entry conflicts with a new one (same index
		//but different terms), delete the existing entry and all that
		//follow it

		// 4. Append any new entries not already in the log

		r.log = r.log[:args.PrevLogIndex+1]
		r.log = append(r.log, args.Entries...)
		reply.Success = true

		if reply.Success {
			if args.LeaderCommit > r.commitIndex {
				r.commitIndex = args.LeaderCommit
			}
		}

		glog.V(1).Infof("raft: %v, log: %v, following leader: %v\n", r.id, r.log, args.LeaderId)
		return nil
	}
}

func (r *Raft) becomeFollower() {
	r.currentState = Follower
	r.votedFor = nil
	r.lastUpdatedFromLeader = time.Now()
	r.randomElectionTimeout = getRandomElectionTimeoutInMillis()
}

func (r *Raft) becomeLeader() {
	r.currentState = Leader
	for i, _ := range r.nextIndex {
		r.nextIndex[i] = len(r.log)
		r.matchIndex[i] = -1
	}
}

func (r *Raft) becomeCandidate() {
	r.currentState = Candidate
}

func (r *Raft) IsLeader() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.currentState == Leader
}

type GetNodeArgs struct {
	Dummy int
}

type GetNodeReply struct {
	IsLeader bool
}

func (r *Raft) GetNode(args GetNodeArgs, reply *GetNodeReply) error {
	reply.IsLeader = r.IsLeader()
	glog.V(2).Infof("raft: %v isLeader: %v\n", r.id, reply.IsLeader)
	return nil
}

func getMajority(N int) int {
	return (N / 2) + 1
}

type RequestVoteArgs struct {
	Term, CandidateId, LastLogIndex, LastLogTerm int
}

type RequestVoteReply struct {
	TermToUpdate int
	VoteGranted  bool
}

type AppendEntriesArgs struct {
	Term, LeaderId, PrevLogIndex, PrevLogTerm int
	Entries                                   []Entry
	LeaderCommit                              int
}

type AppendEntriesReply struct {
	TermToUpdate int
	Success      bool
}

func Make(id int, peers []Peer) *Raft {
	rf := &Raft{}
	rf.id = id
	rf.peers = peers
	rf.lastUpdatedFromLeader = time.Now()
	rf.randomElectionTimeout = getRandomElectionTimeoutInMillis()
	rf.IsAlive = true
	rf.commitIndex = -1
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	go rf.electionTimeout()
	go rf.sendHeartbeatsAsLeader()

	return rf
}
