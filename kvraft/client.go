package raftkv

import "labrpc"
  import "crypto/rand"
  import "math/big"

 import "sync"

  type Clerk struct {
        servers []*labrpc.ClientEnd
        // You will have to modify this struct

	//corresponds to clientID 
        me int64  

	//for locking and unlocking purposes 
        mu sync.Mutex

        //timestamp is for duplicate detection! 
	timestamp int  
  }

//GIVEN 
//basic purpose is to create a random id for clients  
func nrand() int64 {
        max := big.NewInt(int64(1) << 62)
        bigx, _ := rand.Int(rand.Reader, max)
        x := bigx.Int64()
        return x
 }

//This function creates a new clerk (client) object --> initialization 
 func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
        ck := new(Clerk)
        ck.servers = servers
        // You'll have to add code here.

	//assigns client an ID 
        ck.me = nrand()
	//init timestamp to 1! increment later on! 
        ck.timestamp = 1
        return ck
  }

//-------------------------------------------------------------------------------------------------------
//Your kvraft client code (Clerk) should try different kvservers 
//it knows about until one responds positively. As long as a client
//can contact a kvraft server that is a Raft leader in a majority
//partition, its operations should eventually succeed.

 //
 // fetch the current value for a key.
 // returns "" if the key does not exist.
 // keeps trying forever in the face of all other errors.
 //
 // you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.Get", &args, &reply)
 //
 // the types of args and reply (including whether they are pointers)
 // must match the declared types of the RPC handler function's
 // arguments. and reply must be passed as a pointer.
 //
  func (ck *Clerk) Get(key string) string {

        // You will have to modify this function.
        ck.mu.Lock()
	//defer ck.mu.Unlock()
        timestamp := ck.timestamp
        ck.timestamp++
        ck.mu.Unlock()
        
	//infinite loop for all servers  
	for {
        	//check every server until leader is found 
		//need to get value from leader ! 
	        for _, server := range ck.servers {
                
			//create an get Args structure for the RPC call! (from common.go) 
		        args := GetArgs{key, ck.me, timestamp}

			//also create a get reply struct for RPC call! (also from common.go) 
                        reply := GetReply{}

			//RPC CALL! 
                        ok := server.Call("RaftKV.Get", &args, &reply)
                        if ok {
				//if leader (correct!) 
                                if !reply.WrongLeader {

					//returns "" if the key does not exist
                                        switch reply.Err {
                                        case OK:
                                                return reply.Value
                                        case ErrNoKey:
                                                return ""
                                        }
                                }
                        }//if ok
                }//for each server 
        }//for
  }

//-------------------------------------------------------------------------------------------------------
 // shared by Put and Append.
 //
 // you can send an RPC with code like this:
 // ok := ck.servers[i].Call("RaftKV.PutAppend", &args, &reply)
//
 // the types of args and reply (including whether they are pointers)
 // must match the declared types of the RPC handler function's
 // arguments. and reply must be passed as a pointer.

//very similar structure as GET! 
  func (ck *Clerk) PutAppend(key string, value string, op string) {
        // You will have to modify this function.
        ck.mu.Lock()
	//defer ck.mu.Unlock()
        timestamp := ck.timestamp
        ck.timestamp++
        ck.mu.Unlock()

	//infinite loop for all servers
        for {
		//check every server until leader is found
                //need to get value from leader ! 
		//***Basically almost works the same way as Get***
                for _, server := range ck.servers {
                        
			////create an Args structure for the RPC call! (from common.go)
			args := PutAppendArgs{key, value, op, ck.me, timestamp}

			//also create an reply struct for RPC call! (also from common.go)
                        reply := PutAppendReply{}

			//THE RPC CALL! 
                        ok := server.Call("RaftKV.PutAppend", &args, &reply)
                        if ok {
                                
				//if RPC call is successful and it is the correct leader 
				//don't need to return anyhing since we are appending an entry
				if !reply.WrongLeader {
                                        return
                                }
                        }//if 
                }//for
        }//for
  }

//-----------------------------------------------------------------------------------------------------
//GIVEN! 
  func (ck *Clerk) Put(key string, value string) {
        ck.PutAppend(key, value, "Put")
 }

//GIVEN! 
 func (ck *Clerk) Append(key string, value string) {
        ck.PutAppend(key, value, "Append")
 }
