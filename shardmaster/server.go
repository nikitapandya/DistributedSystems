package shardmaster

import (
	"encoding/gob"
	"labrpc"
	"raft"
	"reflect"
	"sync"
	"time"
)

//-----------------------------------------------------------------------------

type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	configs []Config // indexed by config num
	//args are --> num, shards, groups! 

	//Go provides a built-in map type that implements a hash table.
        //initialized by make() in start()
        //make() allocates a hash map data structure and returns a map value that points to it.

        //SAME as kvraft/server.go kvraft struct 
        appliedChans  map[int]chan Op
        clientTimestamps map[int64]int

}

type Op struct {
	// SAME AS kvraft/server.go Op struct.
	Command   string
	ClientID    int64
	Timestamp   int

	//NEW 
	//Corresponds with joinArgs in common.go --> Servers map[int][]string // new GID -> servers mappings
	JoinServers map[int][]string

	//Corresponds to LeaveArgs in common.go --> GIDs []int
	LeaveGIDs   []int
	
	//Coresponds MoveArgs in common.go --> Shard int
	MoveShard   int
	
	//Coresponds MoveArgs in common.go --> GID int
	MoveGID     int
}

//-----------------------------------------------------------------------------

/*
***
JOIN LEAVE AND MOVE function just like 
PUTAPPEND in kvraft/server.go

we are not returning anything and we want to make
sure that the correct leader is putting/appending the op into raft

QUERY is similar to get in kvraft/server.go

***
*/

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	reply.Err = OK

	entry := Op{Command: "Join", ClientID: args.ClientID, Timestamp: args.Timestamp, JoinServers: args.Servers}

	reply.WrongLeader = !sm.sendEntryToLog(entry)
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	reply.Err = OK
	
	entry := Op{Command: "Leave", ClientID: args.ClientID, Timestamp: args.Timestamp, LeaveGIDs: args.GIDs}

	reply.WrongLeader = !sm.sendEntryToLog(entry)
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	reply.Err = OK
	
	entry := Op{Command: "Move", ClientID: args.ClientID, Timestamp: args.Timestamp, MoveShard: args.Shard, MoveGID: args.GID}

	reply.WrongLeader = !sm.sendEntryToLog(entry)
}

//STRUCTURED SIMILAR to Get() in kvraft/server.go 
func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.

	//To ensure the wrong leader will be commit the op
        //want leeader to received our command

	entry := Op{Command: "Query", ClientID: args.ClientID, Timestamp: args.Timestamp}
	isLeader := sm.sendEntryToLog(entry)
	
	if isLeader {
		reply.WrongLeader = false
		sm.mu.Lock()
		defer sm.mu.Unlock()	
	
		//"If the number is -1 or bigger than the biggest known configuration number,
		//the shardmaster should reply with the latest configuration. The result of 
		//Query(-1) should reflect every Join, Leave, or Move RPC that the shardmaster 
		//finished handling before it received the Query(-1) RPC."

		if args.Num == -1 || len(sm.configs) < args.Num {
			reply.Config = sm.configs[len(sm.configs)-1]
		} else {

		//else: The shardmaster replies with the configuration that has that number.
			reply.Config = sm.configs[args.Num]
			reply.Err = OK
		}

	} else {
		reply.WrongLeader = true
	}
}

//-----------------------------------------------------------------------------
//BASICALLY THE SAME AS sendEntryToLog() I wrote in jvraft/server.go
func (sm *ShardMaster) sendEntryToLog(op Op) bool {
	
	//Start puts command as an parameter
        //start returns a int,int,bool --> index,term,isLeader
        //INDEX -->clientID -->  where the op needs to be committed

	index, _, isLeader := sm.rf.Start(op)
	
	//if not a leader --> cannot append entires
	if !isLeader {
		return false
	}

	sm.mu.Lock()
	
	//channel --> need to communicate
        //so we place the op at the correct index
	appliedChan, ok := sm.appliedChans[index]

	if !ok {
		appliedChan = make(chan Op)
		sm.appliedChans[index] = appliedChan
	}
	sm.mu.Unlock()
	
	//JUST AS IN kvraft
	//Channel keeps listening until the correct index and op
        //have been committed (by majority of raft)
        //else a timeout is called!
	
	select {
	case appliedOp := <-appliedChan:
		//return appliedOp == op
		//need to use reflect.DeepEqual cause without it getting this error:
		//"struct containing map[int][]string cannot be compared)"
		//thus add import "reflect" at the top 

		return reflect.DeepEqual(appliedOp, op)

	case <-time.After(300 * time.Millisecond):
		return false
	}
}

//Source for deepEqual
//http://stackoverflow.com/questions/15893250/uncomparable-type-error-when-string-field-used-go-lang
//https://golang.org/pkg/reflect/#DeepEqual


//NEW EXTRA FUNCTIONS 
//-----------------------------------------------------------------------------
//FOR JOIN/LEAVE/MOVE 
//"Whenever this assignment needs to change, the shard master creates a new configuration 
//with the new assignment. Key/value clients and servers contact the shardmaster when they 
//want to know the current (or a past) configuration."

func (sm *ShardMaster) createNewConfig(config Config) Config {
	//Config hasthree parameters: Num, Shards, Groups 
	
	//CREATE A NEW CONFIG OBJECT ! 
	newConfig := Config{}

	//First, we increment the config number!!
	newConfig.Num = config.Num + 1

	//"HINT: Go maps are references. If you assign one variable of type map to another, 
	//both variables refer to the same map. Thus if you want to create a new Config based on a 
	//previous one you need to create a new map object and copy the keys and values individually.
	
	//Second, copy over the shards (GIDs)!
	for index := range config.Shards {
		newConfig.Shards[index] = config.Shards[index]
	}

	//Last, copy over the groups --> servers! 
	newConfig.Groups = map[int][]string{}
	for index, data := range config.Groups {
		newConfig.Groups[index] = data
	}

	return newConfig
}

//"Join and Leave re-balance (configurations)."`
//"The new configuration should divide the shards as evenly as possible among the groups,
//and should move as few shards as possible to achieve that goal."

func (sm *ShardMaster) joinReconfig(config *Config) {
	
	//GET GIDs
	// If you want to create a new Config based on a previous one, need to create a 
	//new map object (with Make()) and copy the keys and values individually.
	//gids := config.Groups
	
	gids := make([]int, 0)
	for gid := range config.Groups {
		gids = append(gids, gid)
	}

	//ASSIGN each shard a GID
	//This loop is the only difference between join and leaveReconfig! 
	//TO preparre for the new groups 
	for shardIndex, gid := range config.Shards {
		if _, ok := config.Groups[gid]; !ok {
			config.Shards[shardIndex] = gids[0]
		}
	}

	//FOR EVERY GID (index i) in this shard increament by 1 
	countShards := map[int]int{}
	for i := 0; i < NShards; i++ {
		countShards[config.Shards[i]]+=1
	}

	//*********
	ratio := (NShards/len(config.Groups))+1
	for i := 0; i <=1 ; i++ {
	for shard, gid := range config.Shards {
		if countShards[gid] > ratio-i {
			for _, newGID := range gids {
				if countShards[newGID] < ratio-i {
					config.Shards[shard] = newGID
					countShards[gid]--
					countShards[newGID]++
					break
		 		} }//for 				
 		}
	}//inner for
	}//outer for
}

func (sm *ShardMaster) leaveReconfig(config *Config) {

	//GET GIDs
        // If you want to create a new Config based on a previous one, need to create a
        //new map object (with Make()) and copy the keys and values individually.
        //gids := config.Groups

        gids := make([]int, 0)
        for gid := range config.Groups {
                gids = append(gids, gid)
        }

	//FOR EVERY GID (index i) in this shard increament by 1
	countShards := map[int]int{}
        for i := 0; i < NShards; i++ {
                countShards[config.Shards[i]]+=1
        }

	ratio := NShards/len(config.Groups)
	for i := 0; i <=1 ; i++ {
	for shard, gid := range config.Shards {
		if _, ok := config.Groups[gid]; !ok {
			for _, newGID := range gids {
				if countShards[newGID] < ratio+i {
					config.Shards[shard] = newGID
					countShards[gid]--
					countShards[newGID]++
					break
			}  }//for 
		}
	}//inner for
	}//outer for
}

//-----------------------------------------------------------------------------

//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
	sm.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

//-----------------------------------------------------------------------------

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}

	gob.Register(Op{})
	sm.applyCh = make(chan raft.ApplyMsg)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)

	// Your code here --> SAME VARS AS USED IN StartKVServer() !! 
	sm.appliedChans = make(map[int]chan Op)
	sm.clientTimestamps = make(map[int64]int)

	//**THIS CHANNEL READS MESSAGES FROM RAFT ***
	sm.applyCh = make(chan raft.ApplyMsg)

	//CREATE A RAFT OBJECT -->using make() in raft.go
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)

	//should start goroutines for any long-running work.
	go func() { 

	for {
                msg := <-sm.applyCh
                if msg.Command == nil {
                        continue
                }
                sm.mu.Lock()
                
		//extract op/entry we got from raft
		op := msg.Command.(Op)

		//Need to make sure this is the correct op we were expecting
                //done by checking timestamps (no duplicates) (in order)
                //the op will tell us if its a join/leave/move/query 
                if sm.clientTimestamps[op.ClientID] < op.Timestamp {
                 
		       switch op.Command {
                       
			 case "Join":
                                
				//The shardmaster should react by creating a new configuration that includes the new replica groups.
				newConfig := sm.createNewConfig(sm.configs[len(sm.configs)-1])
                                for index, data := range op.JoinServers {
                                        newConfig.Groups[index] = data
                                }
                                
				// The new configuration should divide the shards as evenly as possible among the groups,
				//and should move as few shards as possible to achieve that goal.
				sm.joinReconfig(&newConfig)
                                sm.configs = append(sm.configs, newConfig)
                        
			case "Leave":

				//The shardmaster should create a new configuration that does not include those groups,
                        	//and that assigns those groups' shards to the remaining groups.
                                newConfig := sm.createNewConfig(sm.configs[len(sm.configs)-1])
                                for index := range op.LeaveGIDs {
                                        
					//from config.go: delete(cfg.clerks, ck)
					delete(newConfig.Groups, op.LeaveGIDs[index])
                                }

				//The new configuration should divide the shards as evenly as possible among the groups,
				//and should move as few shards as possible to achieve that goal.
                                sm.leaveReconfig(&newConfig)
                                sm.configs = append(sm.configs, newConfig)
                        
			case "Move":

				//the shardmaster should create a new configuration in which the shard is assigned to the group.		
                                //assign the new GID to the new config
				newConfig := sm.createNewConfig(sm.configs[len(sm.configs)-1])
                                newConfig.Shards[op.MoveShard] = op.MoveGID
                                sm.configs = append(sm.configs, newConfig)
                        
			//just like GET() case in kvraft/server.go
			//we do nothing in this case! 
			case "Query":
                        }
                        sm.clientTimestamps[op.ClientID] = op.Timestamp
                }

		 //Check to see we are at the correct index
                 //if yes, send the operation to raft, where it
                //should be adopted by the majority of the servers
                //COMMITTING
		//SAME AS StartKVServer()
                go func(msg raft.ApplyMsg) {
                        sm.mu.Lock()
                        appliedChan, ok := sm.appliedChans[msg.Index]
                        sm.mu.Unlock()
                        if ok {
                                appliedChan <- msg.Command.(Op)
                        }
                }(msg)
                sm.mu.Unlock()
       
	 }//for

	}()
	

	return sm
}	
