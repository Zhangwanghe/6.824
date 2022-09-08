package kvraft

import (
	"crypto/rand"
	"math/big"

	"6.824/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	leaderId     int
	serialNumber int
	clientId     int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	// You'll have to add code here.
	ck.servers = servers
	ck.leaderId = -1
	ck.clientId = nrand()
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	args := GetArgs{key, ck.clientId, ck.serialNumber}
	ck.serialNumber++

	for {
		// todo add lock for this leaderId?
		if ck.leaderId != -1 {
			reply := GetReply{}
			ok := ck.servers[ck.leaderId].Call("KVServer.Get", &args, &reply)
			if ok && reply.Err == "" {
				return reply.Value
			}
		}

		for index, server := range ck.servers {
			reply := GetReply{}
			ok := server.Call("KVServer.Get", &args, &reply)
			if ok && reply.Err == "" {
				ck.leaderId = index
				return reply.Value
			}
		}
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.

	args := PutAppendArgs{key, value, op, ck.clientId, ck.serialNumber}
	ck.serialNumber++

	for {
		if ck.leaderId != -1 {
			reply := PutAppendReply{}
			ok := ck.servers[ck.leaderId].Call("KVServer.PutAppend", &args, &reply)
			if ok && reply.Err == "" {
				return
			}
		}

		for index, server := range ck.servers {
			reply := PutAppendReply{}
			ok := server.Call("KVServer.PutAppend", &args, &reply)
			if ok && reply.Err == "" {
				ck.leaderId = index
				return
			}
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
