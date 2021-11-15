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
	MinWaitInSecs   = 5
	MaxWaitInSecs   = 10
	HeartbeatInSecs = 2
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
	id int
	port string
	client *rpc.Client
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

//type Consensus interface {
//	RequestVote(term, candidateId, lastLogIndex, lastLogTerm int)
//	AppendEntries(term, leaderId, prevLogIndex, prevLogTerm int, entries []Entry, leaderCommit int)
//}

// election timeout goroutine
func (r *Raft) electionTimeout() {
	for {
		r.mu.Lock()
		if r.currentState != Leader {
			r.mu.Unlock()
			randomElectionTimeoutInSecs := getRandomElectionTimeoutInSecs()
			time.Sleep(1 * time.Second)
			r.mu.Lock()
			currentTime := time.Now()
			duration := currentTime.Sub(r.lastUpdatedFromLeader)
			// todo: tune election timeout duration
			if duration.Seconds() > float64(randomElectionTimeoutInSecs) {
				r.becomeCandidate()
				r.mu.Unlock()
				go r.conductElection()
			} else {
				r.mu.Unlock()
			}
		}
		r.mu.Unlock()
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
	lastEntry := r.log[lastLogIndex]
	majority := getMajority(len(r.peers))
	r.mu.Unlock()

	var mu sync.Mutex
	cond := sync.NewCond(&mu)

	for _, peer := range r.peers {
		go func(p Peer) {
			// todo: might have to make lastLogIndex => lastLogIndex + 1
			//termToUpdate, voteGranted := r.RequestVote(currentTermCopy, r.id, lastLogIndex, lastEntry.term)
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
			err := r.RequestVote(args, reply)
			if err != nil {
				log.Printf("RequestVote: args: %+v error: %s\n", args, err.Error())
				//totalReceived += 1
				//cond.Broadcast()
				return
			}
			// re-check assumptions
			mu.Lock()
			defer mu.Unlock()

			if r.currentTerm > currentTermCopy {
				// convert to follower (paper section 5.1)
				r.becomeFollower()
				//totalReceived += 1
				//cond.Broadcast()
				return
			}

			if reply.VoteGranted {
				totalVotes += 1
			} else {
				r.currentTerm = reply.TermToUpdate
			}

			//totalReceived += 1
			//cond.Broadcast()
		}(peer)
	}

	mu.Lock()
	defer mu.Unlock()
	for totalVotes < majority && totalReceived != len(r.peers) {
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
			currentTime := time.Now()
			duration := currentTime.Sub(r.lastHeartbeatSent)
			if duration.Seconds() > float64(HeartbeatInSecs) {
				r.lastHeartbeatSent = time.Now()
				currentTermCopy := r.currentTerm
				commitIndexCopy := r.commitIndex
				for _, peer := range r.peers {
					go func(p Peer) {
						r.mu.Lock()
						totalLogs := len(r.log)
						nextIndex := r.nextIndex[p.id]
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

						//termToUpdate, success := r.AppendEntries(args, &reply)
						err := r.AppendEntries(args, reply)
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
							r.nextIndex[p.id] = totalLogs
							r.matchIndex[p.id] = totalLogs - 1
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
	r.mu.Lock()
	defer r.mu.Unlock()
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
	VoteGranted bool
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

type AppendEntriesArgs struct {
	Term, LeaderId, PrevLogIndex, PrevLogTerm int
	Entries []Entry
	LeaderCommit int
}

type AppendEntriesReply struct {
	TermToUpdate int
	Success bool
}

func (r *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) error {
	panic("implement me")
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
