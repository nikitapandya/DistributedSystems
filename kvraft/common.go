package raftkv

const (
	OK       = "OK"
	ErrNoKey = "ErrNoKey"
)

type Err string

//These args and reply structs are called 
//in client.go in the get and putappend 
//functions !!! 

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.

	//corresponds with me! in client.go
	ClientID  int64

	//timestamp --> for duplicate detection 
	Timestamp int
}

type PutAppendReply struct {
	WrongLeader bool
	Err         Err
	//no need for value since we are "appending" 
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.

	//corresponds with me! in client.go 
	ClientID  int64
	
	//timestamp --> for duplicate detection 
	Timestamp int
}

type GetReply struct {
	WrongLeader bool
	Err         Err
	Value       string
}
