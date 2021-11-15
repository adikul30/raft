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
	MinWaitInSecs   = 3
	MaxWaitInSecs   = 5
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
			randomElectionTimeoutInSecs := getRandomElectionTimeoutInSecs()
			log.Printf("raft: %v, randomElectionTimeoutInSecs: %v\n", r.id, randomElectionTimeoutInSecs)
			//log.Printf("raft: %v,  time.Now().Sub(r.lastUpdatedFromLeader).Seconds(): %v\n", r.id,  time.Now().Sub(r.lastUpdatedFromLeader).Seconds())
			//log.Printf("raft: %v,  float64(randomElectionTimeoutInSecs): %v\n", r.id, float64(randomElectionTimeoutInSecs))
			for time.Now().Sub(r.lastUpdatedFromLeader).Seconds() < float64(randomElectionTimeoutInSecs) {
				r.mu.Unlock()
				time.Sleep(1 * time.Second)
				r.mu.Lock()
			}
			//currentTime := time.Now()
			//duration := currentTime.Sub(r.lastUpdatedFromLeader)
			// todo: tune election timeout duration
			//if duration.Seconds() > float64(randomElectionTimeoutInSecs) {
			log.Printf("raft: %v, candidacy started\n", r.id)
			r.becomeCandidate()
			log.Printf("raft: %v\n", r.currentState)
			r.mu.Unlock()
			go r.conductElection()
			//}
		} else {
			r.mu.Unlock()
		}
	}
}

func getRandomElectionTimeoutInSecs() int {
	rand.Seed(time.Now().UnixNano())
	timeout := rand.Intn(MaxWaitInSecs-MinWaitInSecs+1) + MinWaitInSecs
	return timeout
}

func (r *Raft) conductElection() {
	totalVotes := 0
	totalReceived := 0
	r.mu.Lock()
	r.currentTerm += 1
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

	var mu sync.Mutex
	cond := sync.NewCond(&mu)

	log.Printf("raft: %v, let's get some votes...\n", r.id)

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
			log.Printf("raft: %v, calling RequestVote...\n", r.id)
			err := r.requestVoteRPC(p.Id, args, reply)
			if err != nil {
				log.Printf("RequestVote: args: %+v error: %s\n", args, err.Error())
				return
			}
			log.Printf("raft: %v, RequestVote: args: %v; reply: %v\n", r.id, args, reply)
			// re-check assumptions
			mu.Lock()
			defer mu.Unlock()

			if r.currentTerm > currentTermCopy {
				// convert to follower (paper section 5.1)
				r.becomeFollower()
				return
			}

			if reply.VoteGranted {
				totalVotes += 1
			} else {
				r.currentTerm = reply.TermToUpdate
			}
		}(peer)
	}

	mu.Lock()
	defer mu.Unlock()
	for totalVotes < majority && totalReceived != len(r.peers) - 1 {
		log.Printf("raft: %v, busy waiting for votes...\n", r.id)
		cond.Wait()
	}

	if r.currentState != Candidate {
		// case: received higher term from one of the requestVote responses and changed to follower
		return
	}

	if totalVotes >= majority {
		r.becomeLeader()
	} else {
		// todo: what to do after split vote or losing election?
		r.becomeFollower()
	}
}

// for leader: send heartbeats goroutine
func (r *Raft) sendHeartbeatsAsLeader() {
	for {
		r.mu.Lock()
		if r.currentState == Leader {
			log.Printf("raft: %v, I'm the leader!\n", r.id)
			currentTime := time.Now()
			duration := currentTime.Sub(r.lastHeartbeatSent)
			if duration.Seconds() > float64(HeartbeatInSecs) {
				r.lastHeartbeatSent = time.Now()
				currentTermCopy := r.currentTerm
				commitIndexCopy := r.commitIndex
				for _, peer := range r.peers {
					if peer.Id == r.id {
						continue
					}
					go func(p Peer) {
						r.mu.Lock()
						totalLogs := len(r.log)
						nextIndex := r.nextIndex[p.Id]
						prevLogIndex := nextIndex - 1
						prevLogIndexTerm := r.log[prevLogIndex].Term
						entries := make([]Entry, totalLogs-nextIndex)
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
						err := r.appendEntriesRPC(p.Id, args, reply)
						if err != nil {
							log.Printf("RequestVote: args: %+v error: %s\n", args, err.Error())
							return
						}

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
							r.nextIndex[p.Id] = totalLogs
							r.matchIndex[p.Id] = totalLogs - 1
						}
					}(peer)
				}
			} else {
				r.mu.Unlock()
				time.Sleep(1 * time.Second)
			}
		}
		r.mu.Unlock()
	}
}

func (r *Raft) becomeFollower() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.currentState = Follower
}

func (r *Raft) becomeLeader() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.currentState = Leader
}

func (r *Raft) becomeCandidate() {
	//r.mu.Lock()
	//defer r.mu.Unlock()
	r.currentState = Candidate
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

func (r *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) error {
	r.mu.Lock()
	if args.Term < r.currentTerm {
		reply.VoteGranted = false
		reply.TermToUpdate = r.currentTerm
		r.mu.Unlock()
		return nil
	}

	if args.Term > r.currentTerm {
		r.currentTerm = args.Term
		r.currentState = Follower
	}

	// todo 2B: also check if candidate's log is up-to-date
	if r.votedFor == nil || *r.votedFor == args.CandidateId {
		reply.VoteGranted = true
		reply.TermToUpdate = r.currentTerm
		r.mu.Unlock()
		return nil
	}

	r.mu.Unlock()
	return errors.New("unexpected state! ")
}

func (r *Raft) requestVoteRPC(peerId int, args RequestVoteArgs, reply *RequestVoteReply) error {
	if r.peers[peerId].Client == nil {
		r.constructClientEndpoint(peerId)
	}
	err := r.peers[peerId].Client.Call("Raft.RequestVote", args, reply)
	return err
}

func (r *Raft) constructClientEndpoint(peerId int) {
	client, err := rpc.DialHTTP("tcp", "127.0.0.1:" + r.peers[peerId].Port)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	r.peers[peerId].Client = client
}

func (r *Raft) appendEntriesRPC(peerId int, args AppendEntriesArgs, reply *AppendEntriesReply) error {
	err := r.peers[peerId].Client.Call("Raft.AppendEntries", args, reply)
	return err
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

func (r *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if args.Term < r.currentTerm {
		reply.Success = false
		reply.TermToUpdate = r.currentTerm
		return nil
	}

	// todo 2B: check for other conditions
	return errors.New("unexpected state! ")
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
