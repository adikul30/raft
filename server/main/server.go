package main

import (
	"Raft/raft"
	"flag"
	"fmt"
	_ "github.com/golang/glog"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"strconv"
	"sync"
	"time"
)

const (
	N = 3
)

type config struct {
	rafts []*raft.Raft
}

func init() {
	flag.Parse()
}

func main() {
	var wg sync.WaitGroup
	peers := make([]raft.Peer, N)
	rafts := []*raft.Raft{}

	for i, _ := range peers {
		peers[i].Id = i
		peers[i].Port = "900" + strconv.Itoa(i)
	}

	for i := 0; i < N; i++ {
		raftNode := raft.Make(i, peers)
		rafts = append(rafts, raftNode)
	}

	cfg := config{rafts: rafts}
	fmt.Printf("cfg.rafts: %v\n", cfg.rafts)

	for i, peer := range peers {
		fmt.Println(i)
		wg.Add(1)
		go func(id int, p raft.Peer) {
			handler := rpc.NewServer()
			handler.Register(cfg.rafts[id])
			l, e := net.Listen("tcp", "127.0.0.1:" + p.Port)
			if e != nil {
				log.Fatal("listen error:", e)
			}
			fmt.Printf("listening on %s\n", l.Addr())
			http.Serve(l, handler)
			wg.Done()
		}(i, peer)
	}

	//testReelection(&cfg)

	//testLogAfterFailure(&cfg)

	wg.Wait()
}

// case: start nodes, wait till election, after 3 secs, disable the leader, new leader gets elected
// have the client add logs. after waiting some time, enable previously disabled node.
// ensure node follows the new leader and is up-to-date with the logs added in its absence.
func testLogAfterFailure(cfg *config) {
	var currentLeader *raft.Raft
	for currentLeader == nil {
		totalLeaders := 0
		for _, node := range cfg.rafts {
			if node.IsLeader() {
				currentLeader = node
				totalLeaders += 1
			}
		}
		if totalLeaders > 1 {
			log.Fatalf("Two leaders!!")
		}
		time.Sleep(3 * time.Second)
	}

	time.Sleep(3 * time.Second)
	currentLeader.IsAlive = false
	time.Sleep(45 * time.Second)
	currentLeader.IsAlive = true
}

// case: start nodes, wait till election, after 3 secs, disable the leader, new leader should get elected
func testReelection(cfg *config) {
	var currentLeader *raft.Raft
	for currentLeader == nil {
		totalLeaders := 0
		for _, node := range cfg.rafts {
			if node.IsLeader() {
				currentLeader = node
				totalLeaders += 1
			}
		}
		if totalLeaders > 1 {
			log.Fatalf("Two leaders!!")
		}
		time.Sleep(3 * time.Second)
	}

	time.Sleep( 3 * time.Second)
	currentLeader.IsAlive = false
}
