package main

import (
	"Raft/raft"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
)

func main() {
	var wg sync.WaitGroup
	cliArgs := os.Args[1:]
	peers := make([]raft.Peer, 3)
	for i, peer := range peers {
		peer.Id = i
		peer.Port = "900" + strconv.Itoa(i)
	}
	for i, arg := range cliArgs {
		wg.Add(1)
		go func(i int, port string) {
			raftNode := raft.Make(i, peers)
			handler := rpc.NewServer()
			handler.Register(raftNode)
			l, e := net.Listen("tcp", "127.0.0.1:" + string(port))
			if e != nil {
				log.Fatal("listen error:", e)
			}
			fmt.Printf("port %s listening on %s\n", port, l.Addr())
			http.Serve(l, handler)
			wg.Done()
		}(i, arg)
	}
	wg.Wait()
}