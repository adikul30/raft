package main

import (
	"Raft/raft"
	"fmt"
	"log"
	"net/rpc"
	"strconv"
)

func main() {
	N := 3
	var leaderPort string
	for i := 0; i < N; i++ {
		port := "900" + strconv.Itoa(i)
		client, err := rpc.DialHTTP("tcp", "127.0.0.1:" + string(port))
		if err != nil {
			log.Fatal("dialing:", err)
		}
		// Synchronous call
		args := &raft.GetNodeArgs{Dummy: 99}
		var reply raft.GetNodeReply
		fmt.Println("calling: ", port)
		err = client.Call("Raft.GetNode", args, &reply)
		if err != nil {
			log.Fatal("error:", err)
		}

		if reply.IsLeader {
			leaderPort = port
			log.Printf("found leader with port: %v\n", port)
			break
		}
	}
	if len(leaderPort) == 0 {
		log.Fatalf("cannot found leader")
	}

	client, err := rpc.DialHTTP("tcp", "127.0.0.1:" + string(leaderPort))
	if err != nil {
		log.Fatal("dialing:", err)
	}

	args := &raft.Command{
		Variable: "Y",
		Value:    5,
	}
	var reply raft.ExecuteReply
	err = client.Call("Raft.Execute", args, &reply)
	if err != nil {
		log.Fatalf("error calling Raft.Execute: args: %+v, error: %v\n", args, err)
	}
	fmt.Printf("client: args: %+v, reply: %+v\n", args, reply)
}
