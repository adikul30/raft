# Project for RIT CSCI-652 Dsitributed Systems course

## raft

1. ### Install go

    https://go.dev/doc/install

2. ### Terminal window 1 => Server

    By default, the code uses 3 nodes. Change N in both server and client files to experiment on using more nodes. 
    
    `git clone https://github.com/adikul30/raft`
    
    `cd raft/server/main`

    #### to print only important logs
    
    `go run server.go -logtostderr=true -v=1`

    #### to print detailed logs instead
    
    `go run server.go -logtostderr=true -v=2`

    After a leader has been elected, to add new logs to raft, run the client. 

3. ### Terminal window 2 => Client

    `cd raft/client/main`

    `go run client.go`

