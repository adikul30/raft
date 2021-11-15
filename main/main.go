package main

import (
	"Raft/raft"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

func main() {
	var wg sync.WaitGroup
	cliArgs := os.Args[1:]
	for _, arg := range cliArgs {
		wg.Add(1)
		go func(port string) {
			arith := new(raft.Raft)
			handler := rpc.NewServer()
			handler.Register(arith)
			l, e := net.Listen("tcp", "127.0.0.1:" + string(port))
			if e != nil {
				log.Fatal("listen error:", e)
			}
			fmt.Printf("port %s listening on %s\n", port, l.Addr())
			http.Serve(l, handler)
			wg.Done()
		}(arg)
	}
	wg.Wait()
}