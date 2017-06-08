package shardmaster

//
// Shardmaster clerk.
//

//***
//VERY SIMILAR TO kvraft/client.go 
//basically just adding in the clientID and timestamp 
//***

import "labrpc"
import "time"
import "crypto/rand"
import "math/big"
import "sync" //for locking/unlocking


type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.

	//SAME AS CLERK STRUCT AS KVRAFT/CLIENT.GO 

	//corresponds to clientID
        me int64

        //for locking and unlocking purposes
        mu sync.Mutex

        //timestamp is for duplicate detection!
        timestamp int


}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

//SAME AS MakeClerk() in KVRAFT/CLIENT.GO
func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// Your code here.

        //assigns client an ID
        ck.me = nrand()
        //init timestamp to 1! increment later on!
        ck.timestamp = 1
        return ck

}

func (ck *Clerk) Query(num int) Config {
	
	//Has num,clientID,timeStamp
        //create clientid/timestamp
        //num --> given

	args := &QueryArgs{}
	
	// Your code here.
	//ONLY ADDED the following five lines
	//SAME AS get()/putAppend() from kvraft/clinet.go
	//these same five lines are added to the join/leave/move functions

	args.ClientID = ck.me
	ck.mu.Lock()
	args.Timestamp = ck.timestamp
	ck.timestamp++
	ck.mu.Unlock()

	//EVERYTHING ELSE --> GIVEN

	args.Num = num
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply QueryReply
			ok := srv.Call("ShardMaster.Query", args, &reply)
			if ok && reply.WrongLeader == false {
				return reply.Config
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	
	//Has Servers,clientID,timeStamp
	//create clientid/timestamp
	//servers --> given
	
	args := &JoinArgs{}
	
	// Your code here.
	//same five lines as above 
	//function are added here below 

	args.ClientID = ck.me
	ck.mu.Lock()
	args.Timestamp = ck.timestamp
	ck.timestamp++
	ck.mu.Unlock()

	//EVERYTHING ELSE --> GIVEN

	args.Servers = servers

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply JoinReply
			ok := srv.Call("ShardMaster.Join", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	
	//Has GIDs,clientID,timeStamp
        //create clientid/timestamp
        //GIDs --> given
	
	args := &LeaveArgs{}
	
	// Your code here.
	//same five lines as above
        //function are added here below

	args.ClientID = ck.me
	ck.mu.Lock()
	args.Timestamp = ck.timestamp
	ck.timestamp++
	ck.mu.Unlock()

	//EVERYTHING ELSE --> GIVEN

	args.GIDs = gids

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply LeaveReply
			ok := srv.Call("ShardMaster.Leave", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	
	//Has shard,GIDs,clientID,timeStamp
        //create clientid/timestamp
        //shard/GIDs --> given
	
	args := &MoveArgs{}
	
	// Your code here.
	//same five lines as above
        //function are added here below

	args.ClientID = ck.me
	ck.mu.Lock()
	args.Timestamp = ck.timestamp
	ck.timestamp++
	ck.mu.Unlock()

	//EVERYTHING ELSE --> GIVEN

	args.Shard = shard
	args.GID = gid

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply MoveReply
			ok := srv.Call("ShardMaster.Move", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}
