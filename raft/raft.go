package raft

import (
	"errors"
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
	currentState          State
	id                    int
	peers                 []Peer

	// leader
	nextIndex         []int
	matchIndex        []int
	lastHeartbeatSent time.Time

	// mutex
	mu sync.Mutex
}

// election timeout goroutine
func (r *Raft) electionTimeout() {
	for {
		r.mu.Lock()
		if r.currentState != Leader {
			randomElectionTimeoutInMillis := getRandomElectionTimeoutInMillis()
			log.Printf("raft: %v, currentTerm: %v, randomElectionTimeoutInSecs: %v\n", r.id, r.currentTerm, randomElectionTimeoutInMillis)
			for time.Now().Sub(r.lastUpdatedFromLeader).Milliseconds() < int64(randomElectionTimeoutInMillis) {
				r.mu.Unlock()
				time.Sleep(1 * time.Second)
				r.mu.Lock()
			}

			if r.currentState != Follower {
				r.mu.Unlock()
				continue
			}

			if time.Now().Sub(r.lastUpdatedFromLeader).Milliseconds() < int64(randomElectionTimeoutInMillis) {
				r.mu.Unlock()
				continue
			}

			r.becomeCandidate()
			r.mu.Unlock()
			go r.conductElection()
			//}
		} else {
			r.mu.Unlock()
		}
	}
}

func (r *Raft) conductElection() {
	totalVotes := 0
	totalReceived := 0
	r.mu.Lock()
	if r.currentState == Follower {
		r.mu.Unlock()
		return
	}
	r.currentTerm += 1
	r.votedFor = &r.id
	totalVotes += 1
	r.lastUpdatedFromLeader = time.Now()
	currentTermCopy := r.currentTerm
	lastLogIndex := len(r.log) - 1
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
			if r.currentState != Candidate {
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

			if r.currentState != Candidate {
				return
			}

			if reply.VoteGranted {
				totalVotes += 1
			} else if reply.TermToUpdate > r.currentTerm {
				r.currentTerm = reply.TermToUpdate
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
		r.lastUpdatedFromLeader = time.Now()
		log.Printf("raft: %v suffering from split vote/lost election \n", r.id)
	}
}

func (r *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) error {
	r.mu.Lock()
	if args.Term < r.currentTerm {
		reply.VoteGranted = false
		reply.TermToUpdate = r.currentTerm
		r.mu.Unlock()
		return nil
	}

	if r.votedFor == nil || *r.votedFor == args.CandidateId {
		reply.VoteGranted = true
		r.votedFor = &args.CandidateId
		r.mu.Unlock()
		return nil
	}

	reply.VoteGranted = false
	r.mu.Unlock()
	return nil

	// todo 2B: also check if candidate's log is up-to-date
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
		if r.currentState == Leader {
			log.Printf("raft: %v, I'm the leader for term %v\n", r.id, r.currentTerm)
			currentTime := time.Now()
			duration := currentTime.Sub(r.lastHeartbeatSent)
			if duration.Seconds() > float64(HeartbeatInSecs) {
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
						//r.mu.Lock()
						//totalLogs := len(r.log)
						//nextIndex := r.nextIndex[p.Id]
						//prevLogIndex := nextIndex - 1
						prevLogIndex := 0
						//prevLogIndexTerm := r.log[prevLogIndex].Term
						prevLogIndexTerm := 0
						//entries := make([]Entry, totalLogs-nextIndex)
						entries := make([]Entry, 1)
						//newOnes := r.log[nextIndex:]
						//copy(entries, newOnes)
						//r.mu.Unlock()

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
						//log.Printf("raft: %v, from: %v, AppendEntries: args: %+v; reply: %v\n", r.id, p.Id, args, reply)
						r.mu.Lock()
						defer r.mu.Unlock()
						if !reply.Success {
							if reply.TermToUpdate > r.currentTerm {
								r.currentTerm = reply.TermToUpdate
								r.lastUpdatedFromLeader = time.Now()
								r.becomeFollower()
							} else {
								// todo 2B: handle log inconsistency logic (section 5.3)
							}
						} else {
							//r.nextIndex[p.Id] = totalLogs
							//r.matchIndex[p.Id] = totalLogs - 1
						}
					}(peer)
				}
			} else {
				r.mu.Unlock()
				time.Sleep(500 * time.Millisecond)
			}
		} else {
			r.mu.Unlock()
		}
	}
}

func (r *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	// todo: from Leader?

	if args.Term < r.currentTerm {
		reply.Success = false
		reply.TermToUpdate = r.currentTerm
		return nil
	}

	if args.Term >= r.currentTerm{
		r.currentTerm = args.Term
		r.lastUpdatedFromLeader = time.Now()
		if r.currentState != Follower {
			r.becomeFollower()
		}
		reply.Success = true
		reply.TermToUpdate = args.Term
		log.Printf("raft: %v, changing self to follower, following leader: %v!\n", r.id, args.LeaderId)
		return nil
	}

	// todo 2B: check for other conditions
	return errors.New("unexpected state! ")
}

func (r *Raft) becomeFollower() {
	//r.mu.Lock()
	//defer r.mu.Unlock()
	r.currentState = Follower
}

func (r *Raft) becomeLeader() {
	r.currentState = Leader
}

func (r *Raft) becomeCandidate() {
	//r.mu.Lock()
	//defer r.mu.Unlock()
	r.currentState = Candidate
}

func (r *Raft) IsLeader() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.currentState == Leader
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

func (r *Raft) requestVoteRPC(peerId int, args RequestVoteArgs, reply *RequestVoteReply) error {
	if r.peers[peerId].Client == nil {
		r.constructClientEndpoint(peerId)
	}
	err := r.peers[peerId].Client.Call("Raft.RequestVote", args, reply)
	return err
}

func (r *Raft) appendEntriesRPC(peerId int, args AppendEntriesArgs, reply *AppendEntriesReply) error {
	if r.peers[peerId].Client == nil {
		r.constructClientEndpoint(peerId)
	}
	err := r.peers[peerId].Client.Call("Raft.AppendEntries", args, reply)
	return err
}

func (r *Raft) constructClientEndpoint(peerId int) {
	client, err := rpc.DialHTTP("tcp", "127.0.0.1:" + r.peers[peerId].Port)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	r.peers[peerId].Client = client
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
	go rf.electionTimeout()
	go rf.sendHeartbeatsAsLeader()

	return rf
}
