package raft

import (
	"log"
	"net/rpc"
)

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
