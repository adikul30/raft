package main

import (
	"Raft/raft"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"strconv"
	"sync"
)

const (
	N = 3
)

type config struct {
	rafts []*raft.Raft
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

	fmt.Println("waiting...")

	//for {
	//	totalLeaders := 0
	//	for _, raft := range cfg.rafts {
	//		if raft.IsLeader() {
	//			totalLeaders += 1
	//		}
	//	}
	//	if totalLeaders > 1 {
	//		log.Fatalf("Two leaders!!")
	//	}
	//	time.Sleep(3 * time.Second)
	//}

	wg.Wait()
}