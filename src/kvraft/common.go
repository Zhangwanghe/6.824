package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	SerialNumber int
}

type PutAppendReply struct {
	Err      Err
	LeaderId int
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	SerialNumber int
}

type GetReply struct {
	Err   Err
	Value string
}
