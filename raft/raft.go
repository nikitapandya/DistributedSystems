package raft

import (
//	"log"
        "sync"
        "labrpc"
        "time"
        "math/rand"
	"bytes"
        "encoding/gob"
    )

// —————————————————————————————————————————————————————————————————————————————————————————————————————————————————
//ALL STRUCTS INSPIRED BY FIGURE 2 IN HANDOUT! 

// as each Raft peer becomes aware that Successive log Entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().

type ApplyMsg struct {
        Index       int
        Command     interface{}
        UseSnapshot bool   // ignore for lab2; only used in lab3
        Snapshot    []byte // ignore for lab2; only used in lab3
}

// A Go object implementing a single Raft peer.
type Raft struct {
        mu        sync.Mutex          // Lock to protect shared access to this peer's state
        peers     []*labrpc.ClientEnd // RPC end points of all peers
        persister *Persister          // Object to hold this peer's persisted state
        me        int                 // this peer's index into peers[]

        // persistent state on all servers
	//updated on stable storgae before responding to RPCs
        CurrentTerm int
	 VotedFor int
        Logs []Log

        // volatile state on all servers
        commitIndex int
        lastApplied int

        // volatile state on Leaders
        nextIndex []int
        matchIndex []int

        //EXTRA!! 
        state int  //could be a Leader, Follower, Candidate —> const 
        votes int
        applyCh chan ApplyMsg
	heartbeatCh chan int
//	grantVoteCh chan int 
}

const (
        Leader = iota
        Follower
        Candidate
        )

type Log struct {
    Term int
    Index int
    Command interface{}
}

// example RequestVote RPC arguments structure.
type RequestVoteArgs struct {
    Term int
    CandidateId int
    LastLogIndex int
    LastLogTerm int
}


// example RequestVote RPC reply structure.
type RequestVoteReply struct {
    Term int
    VoteGranted bool
}

type AppendEntriesArgs struct {
    Term int
    LeaderId int
    PrevLogIndex int
    PrevLogTerm int
    Entries []Log
    LeaderCommit int
}

type AppendEntriesReply struct {
    Term int
    NextIndex int 
    Success bool
}

type InstallSnapshotArgs struct {
        Term int
        LeaderId int
        LastIncludedIndex int
        LastIncludedTerm int
        Offset int
        Data []byte
        Done bool
}

type InstallSnapshotReply struct {
        Term int
}

// —————————————————————————————————————————————————————————————————————————————————————————————————————————————————

func (rf *Raft) LogLastTerm() int {
	return rf.Logs[len(rf.Logs)-1].Term
}

func (rf *Raft) LogLastIndex() int {
	return rf.Logs[len(rf.Logs)-1].Index
}

//http://golangcookbook.blogspot.com/2012/11/
//generate-random-number-in-given-range.html
func heartbeatTimeout() int {
	return rand.Intn(300-150) + 150	
}

func electionTimeout() int {
        return rand.Intn(1000-750) + 750
}

func (rf *Raft) GetRaftStateSize() int {
	return rf.persister.RaftStateSize()
}


// —————————————————————————————————————————————————————————————————————————————————————————————————————————————————

// return CurrentTerm and whether this server
// believes it is the Leader.
func (rf *Raft) GetState() (int, bool) {

        var term int
        var isLeader bool

        rf.mu.Lock()
        defer rf.mu.Unlock()

        term = rf.CurrentTerm
        if rf.state == Leader {
                isLeader = true
        }else {
                isLeader = false
        }

        return term, isLeader
}

// —————————————————————————————————————————————————————————————————————————————————————————————————————————————————

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.

func (rf *Raft) persist() {
        w := new(bytes.Buffer)
        e := gob.NewEncoder(w)
        e.Encode(rf.CurrentTerm)
        e.Encode(rf.VotedFor)
        e.Encode(rf.Logs)
        data := w.Bytes()
        rf.persister.SaveRaftState(data)
}


func (rf *Raft) readPersist(data []byte) {
        r := bytes.NewBuffer(data)
        d := gob.NewDecoder(r)
        d.Decode(&rf.CurrentTerm)
        d.Decode(&rf.VotedFor)
        d.Decode(&rf.Logs)
}


// —————————————————————————————————————————————————————————————————————————————————————————————————————————————————

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {


   rf.mu.Lock()
   defer rf.mu.Unlock()
   defer rf.persist()

    reply.VoteGranted = false
    reply.Term = rf.CurrentTerm

    //Condition 1: reply false if term<currentTerm
    if args.Term < rf.CurrentTerm{
    	reply.Term = rf.CurrentTerm
	return
     }

    if rf.state == Leader {
        return
    }

    //if RPC request or response contians term T>currentTerm,
   //set currentTerm = T, convert to follower! 
    if args.Term > rf.CurrentTerm {
        rf.CurrentTerm = args.Term
        rf.VotedFor = -1
	rf.state = Follower
    }

    //Condition 2: If votedFor is null or candidateId, and candidate’s log is at
    //least as up-to-date as receiver’s log, grant vote
     
    reply.Term = rf.CurrentTerm
    upToDate := false

    lastTerm := rf.LogLastTerm()
    lastIndex := rf.LogLastIndex()

    //PAGE 8 LAST PARA IN 5.4.1
    //Raft determines which of two logs is more up-to-date by comparing the
    //index and term of the last entries in the logs. If the logs have last
    //entries with different terms, then the log with the later term is more
    //up-to-date. If the logs end with the same term, then whichever log is 
    //longer is more up-to-date.


   if args.LastLogTerm > lastTerm ||
        (args.LastLogIndex >= lastIndex && args.LastLogTerm == lastTerm) {
        upToDate = true
    } else {
        return
    }

    if ((rf.VotedFor == -1 || rf.VotedFor == args.CandidateId) && upToDate) {
        reply.VoteGranted = true
        rf.VotedFor = args.CandidateId
        rf.state = Follower

        go func() {
              rf.heartbeatCh <- 1
        }()
    }

    return
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
        //MAKE RPC CALL! 
        ok := rf.peers[server].Call("Raft.RequestVote", args, reply)

        rf.mu.Lock()
        defer rf.mu.Unlock()

	 if rf.state != Candidate {
              return ok
         }	

	//if the vote was granted we increment the number of votes and 
	//and check if a majority of peers voted for this server, it becomes a leader
       // When a server becomes a leader, it first inits all nextIndex
       //values to the index just after the last one in its log —> most up to date 

        if ok && reply.VoteGranted {
                if rf.CurrentTerm == args.Term {
                    rf.votes++

		    //checking if majority of peers voted for it! 
                    if rf.votes*2 > len(rf.peers) {
                        rf.state = Leader


                        for i := 0; i < len(rf.peers); i++ {
                            rf.nextIndex[i] = rf.LogLastIndex() + 1
                            rf.matchIndex[i] = 0    
                        }
			
			//Upon election, send init empty AppendEntires --> Heartbeats to each server 
                        go rf.AllAppendEntries()
                    }
                } else {
			if rf.CurrentTerm<reply.Term {
				rf.state = Follower
				rf.CurrentTerm = reply.Term
				rf.VotedFor = -1
		
				rf.persist()

				go func() {
            			 
				      rf.heartbeatCh <- 1
      				  }() 
			}
		}//else
        }//if OK

        return ok
}


func (rf *Raft) AllRequestVote (){
    rf.mu.Lock()
    defer rf.mu.Unlock()

    if rf.state != Candidate {
        return
    }

    lastLogTerm := rf.LogLastTerm()
    lastLogIndex := rf.LogLastIndex()
    args := RequestVoteArgs {
        Term : rf.CurrentTerm,
        CandidateId : rf.me,
        LastLogIndex : lastLogIndex,
        LastLogTerm : lastLogTerm,
    }

    for i := 0; i < len(rf.peers); i++ {
        if i != rf.me {
            go rf.sendRequestVote(i, &args, &RequestVoteReply{})
        }
    }
}

// —————————————————————————————————————————————————————————————————————————————————————————————————————————————————
//Empty AppendEntires ---> Heartbeats
//Conditions 1 & 2 == Consistency check (for empty and non empty entires)
//Conditions 3&4&5 == Pt.B ---> when append entires not empty 

func (rf *Raft) AppendEntries (args *AppendEntriesArgs, reply *AppendEntriesReply){
    
   rf.mu.Lock()
   defer rf.mu.Unlock()

    reply.Success = false
    reply.Term = rf.CurrentTerm
  
    // 1. Reply false if term < currentTerm (§5.1)
    if args.Term< rf.CurrentTerm {
        reply.NextIndex = args.PrevLogIndex + 1
	return
    }

    reply.NextIndex = args.PrevLogIndex + 1

    if rf.state == Leader {
        return
    }

    //if RPC request or reponse contains term T>currentTerm
   //set current term = T and convert to follower!! 
    if args.Term > rf.CurrentTerm {
        rf.CurrentTerm = args.Term
        rf.state = Follower
	rf.VotedFor = -1
	rf.persist()
    }

    go func() {
	rf.heartbeatCh <- 1
    }()

    // 2. Reply false if log doesn’t contain an entry at prevLogIndex
    // whose term matches prevLogTerm (§5.3)
    reply.Term = args.Term

    startIndex := rf.Logs[0].Index
  //to avoid "panic: runtime error: index out of range"
     if rf.LogLastIndex() < args.PrevLogIndex || args.PrevLogIndex < startIndex {
        reply.NextIndex = rf.LogLastIndex()+ 1
        return
     }

    if rf.Logs[args.PrevLogIndex-startIndex].Term != args.PrevLogTerm {
	reply.NextIndex = startIndex + 1
	for i := args.PrevLogIndex-startIndex; i >= 0; i-- {
            if rf.Logs[args.PrevLogIndex-startIndex].Term != rf.Logs[i].Term {
                 reply.NextIndex =  startIndex + i + 1
		 break
            }//if
        }//for
	return
   }//if

   //3. if an existing entry CONFLICTS with a new one 
  //(same index BUT different Terms!!), DELETE 
  //the existing entry and ALL that follow 
   index := 0
   conflictIndex := -1 

   for ; index<len(args.Entries); index++ {
	//NO CONFICT --> means that leader already has 
	//the updated nextIndex for the current raft server 
	if rf.LogLastIndex() < args.PrevLogIndex+1+index {
		break
	} else {
	//YES CONFLICT (same index but different terms)
		if rf.Logs[args.PrevLogIndex+1+index-startIndex].Term != args.Entries[index].Term {
			conflictIndex= args.PrevLogIndex+1+index
			break
		}//if
	}//else
   }//for

   //If YES CONFLICT = DELETE existing entry and ALL that follow 
   if conflictIndex != -1 {
	rf.Logs = rf.Logs[:conflictIndex-startIndex] 
   }

  //4. Append any new entires not already in Log
  for ; index<len(args.Entries); index++ {
	rf.Logs = append(rf.Logs, args.Entries[index])
  }

  rf.persist()

  //since we added a new entry, we update nextIndex
  //for the leader 
  reply.NextIndex = rf.LogLastIndex() +1

  //5. If leaderCommit > commitIndex, set commitIndex =
  //  min(leaderCommit,index of last new entry)
  if args.LeaderCommit > rf.commitIndex {
	rf.commitIndex = min(args.LeaderCommit,rf.LogLastIndex())
        go rf.Committing()
  }//if LC>CI
 
 reply.Success = true
 
}

func min(a, b int) int {
    if a < b {
        return a
    }
    return b
}



func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
    //make RPC call!! 
    ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)

    rf.mu.Lock()
    defer rf.mu.Unlock()


    if len(args.Entries) == 0{

        if reply.Term > rf.CurrentTerm {
            rf.state = Follower
            rf.CurrentTerm = reply.Term
            rf.VotedFor = -1
        }

        if rf.state != Leader {
            return ok
        }

        return ok

    } else if (rf.LogLastIndex() < rf.nextIndex[server]) {
        return ok
    } else {

        if rf.state != Leader {
            return ok
        }
    }//else 
   
    if rf.state != Leader {
        return ok
    }


    if ok && reply.Success && rf.state == Leader {
        // update next index for this server

        if rf.nextIndex[server] < args.PrevLogIndex+1+len(args.Entries) {
            rf.nextIndex[server] = args.PrevLogIndex + 1 + len(args.Entries)
        }

       if rf.matchIndex[server] < rf.nextIndex[server]-1 {
            // update match index
            rf.matchIndex[server] = rf.nextIndex[server] - 1
        }

	//If there exists an N such that N > commitIndex, a majority
	//of matchIndex[i] ≥ N, and log[N].term == currentTerm: 
	//set commitIndex = N (§5.3, §5.4).
	
       N := rf.matchIndex[server]
       startIndex := rf.Logs[0].Index


        isMajority := false
        counter := 1
        for j := 0; j < len(rf.peers); j++ {
            if j != rf.me && rf.matchIndex[j] >= N {
                counter++
            }
        }

        if counter > (len(rf.peers)/2) {
            isMajority = true
        }

        if N > rf.commitIndex && rf.Logs[N-startIndex].Term == rf.CurrentTerm && isMajority {
            rf.commitIndex = N
            go rf.Committing()
        }

        return ok
    }

    if ok && !reply.Success {
        if reply.Term > rf.CurrentTerm {
            rf.state = Follower
            rf.CurrentTerm = reply.Term
            rf.VotedFor = -1

	    rf.persist()
 	    return ok

        } else if rf.state == Leader {
            rf.nextIndex[server] = reply.NextIndex
        }
    }
    return ok
}


func (rf *Raft) Committing() {

           rf.mu.Lock()
           defer rf.mu.Unlock()
           
	   startIndex := rf.Logs[0].Index

	   //All Servers: Rule 1: 
  	   //if commitIndex?lastApplied; increment lastApplied, 
	   //apply log[lastApplied] to the state machine 
           //if index of last log entry applied is less than last log entry committed
           

	   for rf.commitIndex > rf.lastApplied {
              rf.lastApplied++  
	      msg := ApplyMsg{Index: rf.lastApplied,
                                         Command: rf.Logs[rf.lastApplied-startIndex].Command,
					 UseSnapshot: false,
                                        }
                rf.applyCh <- msg
           }//for
}

//Helps all servers, append entires 
func (rf *Raft) AllAppendEntries (){

     rf.mu.Lock()
    defer rf.mu.Unlock()

    if rf.state != Leader {
        return
    }

    for i := 0; i < len(rf.peers); i++{
        if i != rf.me && rf.state==Leader  {

            startIndex := rf.Logs[0].Index
            nextIndex := rf.nextIndex[i]
            if nextIndex <= startIndex {
		args := InstallSnapshotArgs{Term: rf.CurrentTerm, 
			LastIncludedIndex: rf.Logs[0].Index, 
			LastIncludedTerm: rf.Logs[0].Term, 
			Data: rf.persister.ReadSnapshot(),
		}
		go rf.sendInstallSnapshot(i, &args, &InstallSnapshotReply{})
	    } else {
		args := AppendEntriesArgs{rf.CurrentTerm, rf.me, nextIndex - 1, rf.Logs[nextIndex-1-startIndex].Term, rf.Logs[nextIndex-startIndex:], rf.commitIndex}
		go rf.sendAppendEntries(i, &args, &AppendEntriesReply{})
	   } 
        }
    }

      go func() {
          rf.heartbeatCh <- 1
      }()

}

// —————————————————————————————————————————————————————————————————————————————————————————————————————————————————
//The leader uses a new RPC called InstallSnapshot to send snapshots to followers 
//When a follower receives a snapshot with this RPC, it must decide what to do with existing entries. 
//if snapshot has info not in follower's log the follower discards its entire log; 
//If instead the follower receives a snapshot that describes a prefix of its log then entries are retained.
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {

	//Cond1: Reply immediately if term < currentTerm
	if args.Term < rf.CurrentTerm {
		reply.Term = rf.CurrentTerm
		return 
	}
	
	rf.mu.Lock()
	defer rf.mu.Unlock()

	//another basic consistency check 
	rf.state = Follower
	if args.Term > rf.CurrentTerm {
		rf.CurrentTerm = args.Term
		rf.VotedFor = -1
		rf.persist()
	}

	go func() {
              rf.heartbeatCh <- 1
        }()


	//if last included index is less than the index of the first log entry --> return
	//snapshot already taken, follower needs to update term --> no need for new snapshot 
	if args.LastIncludedIndex < rf.Logs[0].Index {
		reply.Term = rf.CurrentTerm
		return 
	}

	//if len of logs is the same as snapshot and if index/term
	//of the log is same as snapshot's (lastIncludedIndex/lastIncludedTerm
	//then current snapshot is still valid and no need to create a new snapshot 
	
	//begining of a snapshot 
	startIndex := rf.Logs[0].Index
	if len(rf.Logs) > args.LastIncludedIndex-startIndex && rf.Logs[args.LastIncludedIndex-startIndex].Index == args.LastIncludedIndex && rf.Logs[args.LastIncludedIndex-startIndex].Term == args.LastIncludedTerm {
		reply.Term = rf.CurrentTerm
		return
	}

	//if the past two if condidtions fail --> need to create a new snapshot 
	//Logs[0] serves as the start index of the snapshot 
	//COND2: Create new snapshot file if first chunk (offset is 0)
	rf.Logs = make([]Log, 1)
	rf.Logs[0] = Log{Term: args.LastIncludedTerm, Index: args.LastIncludedIndex}

	//"You must store each snapshot in the persister object using SaveSnapshot()"
	rf.persister.SaveSnapshot(args.Data)
	//need to decode snapshot data before we can send over channel. 
	//COND3: Write data into snapshot file at given offset

	rf.GetSnapshotData(args.Data)
	rf.persist()

	//after snapshot is created --> must send over channel 
	//KVraft server checks channel applyCh full of raft ApplyMsgs 
	applyMsg := ApplyMsg{UseSnapshot: true, Snapshot: args.Data}
	rf.applyCh <- applyMsg
	reply.Term = rf.CurrentTerm
}


func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	//if RPC call successful 
	if ok {
		//same if condition that has been in all sendArgs
		if rf.CurrentTerm < reply.Term {
			rf.state = Follower
			rf.CurrentTerm = reply.Term
			rf.VotedFor = -1
			rf.persist()
		} else {

			//if snapshot has been successfully send to the followers			
			//update the leader's next index to be the first index 
			//index of the snapshot
			if rf.state == Leader {
				rf.nextIndex[server] = rf.Logs[0].Index + 1
			}
		}//else
	}//if
}


//Called whenever we need to discard and reset the current log/snapshot!! 
func (rf *Raft) ResetLog(index int, term int) {
	startIndex := rf.Logs[0].Index

	//if length of log is less than the LastIncludedIndex
	//(Already snapshotted) Cond7&8: Discard and reset log

	if len(rf.Logs) <= index-startIndex {
		rf.Logs = make([]Log, 1)
		rf.Logs[0] = Log{Index: index, Term: term}
	} else {

		//Cond6: If existing log entry has same index and term as snapshot’s
		//last included entry, retain log entries following it and reply

		if rf.Logs[index-startIndex].Index == index && rf.Logs[index-startIndex].Term == term {
			rf.Logs = rf.Logs[index-startIndex:]
		} else {

			//Cond7&8: Discard and reset log
			
			rf.Logs = make([]Log, 1)
			rf.Logs[0] = Log{Index: index, Term: term}
		}
	}//else
}


//Decodes the array of bytes (snapshot data) and calls
//resetLog to see if we should retain this data or discard
//functions in a similar way as readPersist()
//which reads raft state while this reads snapshot state
func (rf *Raft) GetSnapshotData(snapshot []byte) {
        if len(snapshot) <= 0 {
                return
        }
        var LastIncludedIndex int
        var LastIncludedTerm int
        r := bytes.NewBuffer(snapshot)
        d := gob.NewDecoder(r)
        d.Decode(&LastIncludedIndex)
        d.Decode(&LastIncludedTerm)
        rf.lastApplied = LastIncludedIndex
        rf.ResetLog(LastIncludedIndex, LastIncludedTerm)
}


// —————————————————————————————————————————————————————————————————————————————————————————————————————————————————
func (rf *Raft) Start(command interface{}) (int, int, bool) {
        index := -1
        Term := -1
        isLeader := true

        rf.mu.Lock()
	defer rf.mu.Unlock()

	Term = rf.CurrentTerm
	isLeader = rf.state == Leader 

	if rf.state == Leader {
		index = rf.LogLastIndex()+ 1

		//Leaders: Condition: 
		//if command recieved from client; append entry to local log 
		rf.Logs = append(rf.Logs, Log{rf.CurrentTerm,index,command})
		
		rf.persist()
	
		//Start --> send first heartbeat ! 
		go rf.AllAppendEntries()
	}

        return index, Term, isLeader
}

func (rf *Raft) StartSnapshot(dbData []byte, LastIncludedIndex int) {
        rf.mu.Lock()
        defer rf.mu.Unlock() 
        if LastIncludedIndex > rf.Logs[0].Index {
                startIndex := rf.Logs[0].Index
                LastIncludedTerm := rf.Logs[LastIncludedIndex-startIndex].Term
                w := new(bytes.Buffer)
                e := gob.NewEncoder(w)
                e.Encode(LastIncludedIndex)
                e.Encode(LastIncludedTerm)
                data := w.Bytes()
                data = append(data, dbData...)
                rf.persister.SaveSnapshot(data)
                rf.lastApplied = LastIncludedIndex
                rf.ResetLog(LastIncludedIndex, LastIncludedTerm)
                rf.persist()
        }
}


// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.

func (rf *Raft) Kill() {
        // Your code here, if desired.
}

// —————————————————————————————————————————————————————————————————————————————————————————————————————————————————

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
        persister *Persister, applyCh chan ApplyMsg) *Raft {
        rf := &Raft{}
        rf.peers = peers
        rf.persister = persister
        rf.me = me

        // Your initialization code here (2A, 2B, 2C).
        rf.state = Follower
        rf.CurrentTerm = 0
        rf.VotedFor = -1
        rf.Logs = make([]Log, 1)
        rf.Logs[0] = Log{Term :0, Index: 0}
        rf.commitIndex = 0
        rf.lastApplied = 0
        rf.nextIndex = make([]int, len(rf.peers))
        rf.matchIndex = make([]int, len(rf.peers))

        for i := 0; i < len(rf.peers); i++ {
            rf.nextIndex[i] = 1
            rf.matchIndex[i] = 0
        }

        rf.applyCh = applyCh   // not for 2A
	rf.heartbeatCh = make(chan int) 

        // initialize from state persisted before a crash
        rf.readPersist(persister.ReadRaftState())
	rf.GetSnapshotData(persister.ReadSnapshot())
	go func() {
		msg := ApplyMsg{UseSnapshot: true, Snapshot: persister.ReadSnapshot()}
		applyCh <- msg
	}()



        // Infinite loop, for each server:
        // if a Leader: wait for a random time then send heartbeats
        // then, call AllAppendEnteries(), for each server, and then call sendAppendEntries()
        // if a Candidate or Follower: wait for a random time (electionTimeout) then prepare for elections
        // that is update info, then call AllRequestVote() and then for each server, call sendRequestVote()

	go func() {

	for {
	    rf.mu.Lock()
            switch rf.state {
                case Leader:
		rf.mu.Unlock()
                select {
                    case <- rf.heartbeatCh:
		 
                    case <- time.After(time.Duration(heartbeatTimeout()) * time.Millisecond):
                        rf.AllAppendEntries()
                }
                case Follower , Candidate:
		rf.mu.Unlock()
                select {
                    case <- rf.heartbeatCh:
	
                    case <- time.After(time.Duration(electionTimeout()) * time.Millisecond):
                            rf.mu.Lock()

                            //if election timeout happens w/o receiving append entires
                            //RPC from current leader or granting vote to candidate --> convert to candidate
                            rf.state = Candidate
                            rf.votes = 1
                            rf.CurrentTerm++
                            rf.VotedFor = rf.me

			    rf.persist()
                            rf.mu.Unlock()

                            rf.AllRequestVote()
                }//select
            }//switch
        }//for


	}()

return rf

}
