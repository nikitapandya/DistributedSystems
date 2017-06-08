 package raftkv
 
 import (
 	"bytes"
	"encoding/gob"
 	"labrpc"
  	"log"
  	"raft"
  	"sync"
 	"time"
  )
  
  const Debug = 0
 
 func DPrintf(format string, a ...interface{}) (n int, err error) {
 	if Debug > 0 {
 		log.Printf(format, a...)
 	}
  	return
  }
  
//--------------------------------------------------------------------------------------------------

//A reasonable plan of attack may be to first fill in the Op struct in 
//server.go with the "value" information that kvraft will use Raft to agree on 
//(remember that Op field names must start with capital letters, since they will be sent through RPC),
type Op struct {
  	// Your definitions here.
  	// Field names must start with capital letters,
  	// otherwise RPC will break.
 	Key string
 	Value string
 	Command string
 	ClientID int64
 	Timestamp int
  }
  
  type RaftKV struct {
 	mu      sync.Mutex
 	me      int
 	rf      *raft.Raft
 	applyC chan raft.ApplyMsg
 
  	maxraftstate int // snapshot if log grows this big
  
  	// Your definitions here.
 	//Go provides a built-in map type that implements a hash table.
	//initialized by make() in start() 
	//make() allocates a hash map data structure and returns a map value that points to it. 
	
	DB  map[string]string
 	appliedChans  map[int]chan Op
 	ClientTimestamps map[int64]int
  }

//--------------------------------------------------------------------------------------------------

//and then implement the PutAppend() and Get() handlers in server.go.
//The handlers should enter an Op in the Raft log using Start(), and should
//reply to the client when that log entry is committed. Note that you cannot
//execute an operation until the point at which it is committed in the log
//(i.e., when it arrives on the Raft applyCh).
  
  func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {
  	// Your code here.
 	
	//To ensure the wrong leader will be commit the op
	//want leeader to received our command 
	
	entry := Op{Key: args.Key, Command: "Get", ClientID: args.ClientID, Timestamp: args.Timestamp}
	isLeader := kv.sendEntryToLog(entry)
 	
	//if correct leader --> return value from index the KV array by the key 
	//else return WRONG leader!! 
	if isLeader {
		reply.WrongLeader = false
		kv.mu.Lock()
 		defer kv.mu.Unlock()
 		if val, ok := kv.DB[args.Key]; ok {
 			reply.Err = OK
 			reply.Value = val
 		} else {
 			reply.Err = ErrNoKey
 		}
 	} else {
 		reply.WrongLeader = true
 	}
  }


//--------------------------------------------------------------------------------------------------
  
  func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
  	// Your code here.
 	
	//Works very similarly to get! except we are not returning anything 
	//still make sure that the correct leader is putting/appending the op into raft
	reply.Err = OK
 	
	entry := Op{args.Key, args.Value, args.Op, args.ClientID, args.Timestamp}
	reply.WrongLeader = !kv.sendEntryToLog(entry)
}
//--------------------------------------------------------------------------------------------------
//CONNECTION between kv and rf 
 func (kv *RaftKV) sendEntryToLog(op Op) bool {
        // DPrintf("Ready to sendEntryToLog: %v\n", op)
        //Start puts command as an parameter
        //start returns a int,int,bool --> index,term,isLeader 
	
	//INDEX -->clientID -->  where the op needs to be committed  
        index, _, isLeader := kv.rf.Start(op)
        
        //if not a leader --> cannot append entires
        if !isLeader || index == -1  { 
                return false
        }

	//channel --> need to communicate
	//so we place the op at the correct index  
        kv.mu.Lock() 
        appliedChan, ok := kv.appliedChans[index]
	
	if !ok {
                appliedChan = make(chan Op)
                kv.appliedChans[index] = appliedChan
        }
	kv.mu.Unlock()
        
	//Channel keeps listening until the correct index and op 
	//have been committed (by majority of raft) 
	//else a timeout is called! 
	select {
        case appliedOp := <-appliedChan:
                return appliedOp == op
        case <-time.After(300 * time.Millisecond):
                return false
        }
 }

//--------------------------------------------------------------------------------------------------

func (kv *RaftKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

//--------------------------------------------------------------------------------------------------

// servers[] contains the ports of the set of
 // servers that will cooperate via Raft to
 // form the fault-tolerant key/value service.
 // me is the index of the current server in servers[].
 // the k/v server should store snapshots with persister.SaveSnapshot(),
 // and Raft should save its state (including log) with persister.SaveRaftState().
 // the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
 // in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
 // you don't need to snapshot.
 // StartKVServer() must return quickly, so it should start goroutines
 // for any long-running work.
 //
 func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *RaftKV {
 	// call gob.Register on structures you want
 	// Go's RPC library to marshall/unmarshall.
 	gob.Register(Op{})
 
 	kv := new(RaftKV)
 	kv.me = me
  	kv.maxraftstate = maxraftstate
  
  	// Your initialization code here.
 	kv.DB = make(map[string]string)
 	kv.appliedChans = make(map[int]chan Op)
 	kv.ClientTimestamps = make(map[int64]int)
  
	//**THIS CHANNEL READS MESSAGES FROM RAFT ***
  	kv.applyCh = make(chan raft.ApplyMsg)
  	
	//CREATE A RAFT OBJECT -->using make() in raft.go 
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
  
	//should start goroutines for any long-running work.
 	go func() { 

	for {

		//extract a raft message from the applyCh
		msg := <-kv.applyCh
		kv.mu.Lock()

		//if a snapshot is being used!!! 
		//load log configurations --> tell other servers about the snapshot being used 
		//similar function to GetSnapshotData() in raft.go 
		if msg.UseSnapshot {
			var LastIncludedIndex int
			var LastIncludedTerm int
			r := bytes.NewBuffer(msg.Snapshot)
			d := gob.NewDecoder(r)
			d.Decode(&LastIncludedIndex)
			d.Decode(&LastIncludedTerm)
			d.Decode(&kv.DB)
			d.Decode(&kv.ClientTimestamps)
		} else {
			
			//extract op/entry we got from raft
			op := msg.Command.(Op)

			//Need to make sure this is the correct op we were expecting 
			//done by checking timestamps (no duplicates) (in order) 
			//the op will tell us if its a get, putt or append
			//put/append value in KV array according to key 
			if kv.ClientTimestamps[op.ClientID] < op.Timestamp {
				switch op.Command {
				case "Get":
				case "Put":
					kv.DB[op.Key] = op.Value
				case "Append":
					kv.DB[op.Key] += op.Value
				}

				//also update timestamp so we know what to expect from the next op
				kv.ClientTimestamps[op.ClientID] = op.Timestamp
			}

			//***A kvraft server should not complete a Get() RPC if it is not part of a
			// majority (so that it does not serve stale data). Thus, enter every Get() (
			// and put()/append() in the Raft log.***

			
			//Check to see we are at the correct index 
			//if yes, send the operation to raft, where it 
			//should be adopted by the majority of the servers 
			//COMMITTING 
			go func(msg raft.ApplyMsg) {
				kv.mu.Lock()
				appliedChan, ok := kv.appliedChans[msg.Index]
				kv.mu.Unlock()
			
				//Note that you cannot execute an operation 
				//until the point at which it is committed in the log
				if ok {
					appliedChan <- msg.Command.(Op)
				}
			}(msg)


			 // the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
 			// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
 			// you don't need to snapshot.
			//***after an op is committed, need to start a Snapshot***
			//check these conditions after a commit  	
		
			if kv.rf.GetRaftStateSize() > kv.maxraftstate && kv.maxraftstate > 0 {
				w := new(bytes.Buffer)
				e := gob.NewEncoder(w)
				e.Encode(kv.DB)
				e.Encode(kv.ClientTimestamps)
				data := w.Bytes()
				go kv.rf.StartSnapshot(data, msg.Index)
			}
		}
		kv.mu.Unlock()


        }//for
	} ()
  
  	return kv
  }
