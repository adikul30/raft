# Project for RIT CSCI-652 Distributed Systems course

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


### References

1. 
```@inproceedings{ongaro2014search,
      title={In search of an understandable consensus algorithm},
      author={Ongaro, Diego and Ousterhout, John},
      booktitle={2014 $\{$USENIX$\}$ Annual Technical Conference ($\{$USENIX$\}$$\{$ATC$\}$ 14)},
      pages={305--319},
      year={2014}
    }
```
2. Excellent MIT OCW (https://pdos.csail.mit.edu/6.824/schedule.html) lectures and labs. 
3. This awesome vizualization (http://thesecretlivesofdata.com/raft/)
