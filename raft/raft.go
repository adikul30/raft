package raft

import (
	"errors"
	"fmt"
	"log"
	"math/rand"
	"net/rpc"
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

	r.mu.Unlock()
	commitIndex, isReplicated := r.replicateCommandToFollowers(false)
	reply.Index = commitIndex
	reply.IsReplicated = isReplicated
	return nil
}

// election timeout goroutine
func (r *Raft) electionTimeout() {
	for {
		r.mu.Lock()
		if r.currentState != Leader && r.IsAlive {
			log.Printf("raft: %v, currentTerm: %v, lastUpdatedDiff: %v, randomElectionTimeoutInSecs: %v\n", r.id, r.currentTerm, time.Now().Sub(r.lastUpdatedFromLeader).Milliseconds(), r.randomElectionTimeout)
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
	lastLogIndex := len(r.log)
	var lastEntry Entry
	if len(r.log) == 0 {
		lastLogIndex = 0
		lastEntry = Entry{Term: 0}
	} else {
		lastLogIndex = len(r.log) - 1
		lastEntry = r.log[lastLogIndex]
	}

	majority := getMajority(len(r.peers))
	r.mu.Unlock()

	cond := sync.NewCond(&r.mu)

	log.Printf("raft: %v, let's get some votes for term: %v\n", r.id, r.currentTerm)

	for _, peer := range r.peers {
		if peer.Id == r.id {
			continue
		}
		go func(p Peer) {
			// todo: might have to make lastLogIndex => lastLogIndex + 1
			defer func() {
				totalReceived += 1
				cond.Broadcast()
			}()
			args := RequestVoteArgs{
				Term:         currentTermCopy,
				CandidateId:  r.id,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastEntry.Term,
			}

			reply := &RequestVoteReply{}
			if r.currentState != Candidate && r.IsAlive {
				return
			}
			err := r.requestVoteRPC(p.Id, args, reply)
			if err != nil {
				log.Printf("raft: %v, from: %v, RequestVote: args: %+v error: %s\n", r.id, p.Id, args, err.Error())
				return
			}
			log.Printf("raft: %v, from: %v, RequestVote: args: %+v; reply: %v\n", r.id, p.Id, args, reply)

			r.mu.Lock()
			defer r.mu.Unlock()

			if r.currentState != Candidate && r.IsAlive {
				return
			}

			if reply.VoteGranted {
				totalVotes += 1
			} else if reply.TermToUpdate > r.currentTerm {
				r.currentTerm = reply.TermToUpdate
				// todo: fall back to follower?
				//r.becomeFollower()
				// r.lastUpdatedFromLeader = time.Now()
				//r.votedFor = nil
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
		log.Printf("raft: %v won election for term: %v \n", r.id, r.currentTerm)
		r.becomeLeader()
	} else {
		// todo: what to do after split vote or losing election?
		r.becomeFollower()
		log.Printf("raft: %v suffering from split vote/lost election \n", r.id)
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

	if (r.votedFor == nil || *r.votedFor == args.CandidateId) && args.LastLogIndex >= len(r.log) {
		reply.VoteGranted = true
		r.votedFor = &args.CandidateId
		return nil
	}

	reply.VoteGranted = false
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
			log.Printf("raft: %v, log: %+v, I'm the leader for term %v\n", r.id, r.log, r.currentTerm)
			currentTime := time.Now()
			duration := currentTime.Sub(r.lastHeartbeatSent)
			if duration.Seconds() > float64(HeartbeatInSecs) {
				r.lastHeartbeatSent = time.Now()
				r.mu.Unlock()
				_, _ = r.replicateCommandToFollowers(true)
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

func (r *Raft) replicateCommandToFollowers(isHeartbeat bool) (int, bool) {
	r.mu.Lock()
	r.lastHeartbeatSent = time.Now()
	currentTermCopy := r.currentTerm
	commitIndexCopy := r.commitIndex
	r.mu.Unlock()

	for _, peer := range r.peers {
		if peer.Id == r.id {
			continue
		}
		go func(p Peer) {
			//log.Printf("raft: %v, sending heartbeat to %v!\n", r.id, p.Id)
			r.mu.Lock()
			totalLogs := len(r.log)
			var prevLogIndexTerm, prevLogIndex int
			var entries []Entry

			if totalLogs == 0 {
				prevLogIndex = 0
				prevLogIndexTerm = 0
			} else if len(r.log) >= r.nextIndex[p.Id] {
				nextIndex := r.nextIndex[p.Id]
				prevLogIndex = nextIndex - 1
				if prevLogIndex == 0 { // case for first ever entry in the log to be replicated
					prevLogIndexTerm = 0
				} else {
					prevLogIndexTerm = r.log[prevLogIndex].Term
				}
				if !isHeartbeat {
					entries = make([]Entry, totalLogs-prevLogIndex)
					newOnes := r.log[prevLogIndex:]
					copy(entries, newOnes)
				}
			}
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
			//log.Printf("raft: %v, calling appendEntriesRPC for %v!\n", r.id, p.Id)
			err := r.appendEntriesRPC(p.Id, args, reply)
			if err != nil {
				log.Printf("AppendEntries: args: %+v error: %s\n", args, err.Error())
				return
			}
			log.Printf("raft: %v, isHeartbeat: %v, from: %v, AppendEntries: args: %+v; reply: %v\n", r.id, isHeartbeat, p.Id, args, reply)
			r.mu.Lock()
			defer r.mu.Unlock()
			if !reply.Success {
				if reply.TermToUpdate > r.currentTerm {
					r.currentTerm = reply.TermToUpdate
					r.becomeFollower()
				} else {
					// todo 2B: handle log inconsistency logic (section 5.3)
				}
			} else {
				r.nextIndex[p.Id] = totalLogs + 1
				r.matchIndex[p.Id] = r.nextIndex[p.Id] - 1
				// todo: can place call to updateCommitIndex() here
			}
		}(peer)
	}
	r.updateCommitIndex()
	// todo: check majority of responses before returning true
	return -1, false
}

func (r *Raft) updateCommitIndex() {
	//r.mu.Lock()
	//defer r.mu.Unlock()

	N := r.matchIndex[0]
	for _, index := range r.matchIndex {
		if index < N {
			N = index
		}
	}

	if N > r.commitIndex && r.log[N].Term == r.currentTerm {
		r.commitIndex = N
	}
}

func (r *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if !r.IsAlive {
		return errors.New("not alive! ")
	}

	if args.Term < r.currentTerm {
		reply.Success = false
		reply.TermToUpdate = r.currentTerm
		return nil
	}

	if len(args.Entries) == 0 {
		// heartbeat
		r.currentTerm = args.Term
		r.becomeFollower()
		reply.Success = true
		log.Printf("raft: %v, log: %+v, following leader: %v\n", r.id, r.log, args.LeaderId)
		return nil
	}

	if len(r.log) == 0 {
		// first entry
		r.log = append(r.log, args.Entries...)
		reply.Success = true
	} else {
		if r.log[args.PrevLogIndex - 1].Term != args.PrevLogTerm {
			reply.Success = false
		} else {
			// todo: receiver implementation: might not work as combined pt.3 and 4
			r.log = r.log[:args.PrevLogIndex]
			r.log = append(r.log, args.Entries...)
			reply.Success = true
		}
	}

	if reply.Success {
		if args.LeaderCommit > r.commitIndex {
			r.commitIndex = args.LeaderCommit
		}
	}

	log.Printf("raft: %v, log: %+v, following leader: %v\n", r.id, r.log, args.LeaderId)
	return nil
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
		r.nextIndex[i] = len(r.log) + 1
		r.matchIndex[i] = 0
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
	fmt.Printf("raft: %v isLeader: %v\n", r.id, reply.IsLeader)
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
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	go rf.electionTimeout()
	go rf.sendHeartbeatsAsLeader()

	return rf
}
