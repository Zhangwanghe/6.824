package shardkv

import (
	"bytes"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"6.824/shardctrler"
)

const Debug = false

func DPrintf(index int, format string, a ...interface{}) (n int, err error) {
	if Debug {
		time := time.Now()
		timeFormat := "2006-01-02 15:04:05.000"
		prefix := fmt.Sprintf("%s S%d ", time.Format(timeFormat), index)
		format = prefix + format
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OpType       string
	Key          string
	Value        string
	Client       int64
	SerialNumber int
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big
	sm           *shardctrler.Clerk

	// Your definitions here.
	dead                  int32 // set by Kill()
	persister             *raft.Persister
	Appliedlogs           map[int]interface{}
	Requiredlogs          map[int]int
	ClientKeySerialNumber map[int64]map[string]int
	CommitIndex           int
	checkedLeader         bool
	KeyValues             map[string]string
	config                shardctrler.Config
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	if !kv.keyInGroup(args.Key) {
		DPrintf(kv.me, "Get !keyInGroup args = %+v\n", args)
		reply.Err = ErrWrongGroup
		return
	}

	ok, val := kv.hasExecuted(args.Client, args.SerialNumber, args.Key)
	if ok {
		DPrintf(kv.me, "Get hasExecuted\n")
		reply.Value = val
		reply.Err = OK
		return
	}

	if !kv.canExecute(args.Client, args.SerialNumber, args.Key) {
		DPrintf(kv.me, "Get !canExecute args = %+v\n", args)
		reply.Err = ErrWrongOrder
		return
	}

	op := Op{"Get", args.Key, "", args.Client, args.SerialNumber}
	err := kv.startAndWaitForOp(op)
	if err != OK {
		reply.Err = err
		return
	} else if !kv.keyInGroup(args.Key) {
		reply.Err = ErrWrongGroup
		return
	}

	reply.Value = kv.GetVal(args.Key)
	reply.Err = OK
}

func (kv *ShardKV) GetVal(key string) string {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	val, ok := kv.KeyValues[key]
	if !ok {
		return ""
	}

	return val
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	if !kv.keyInGroup(args.Key) {
		DPrintf(kv.me, "PutAppend !keyInGroup args = %+v\n", args)
		reply.Err = ErrWrongGroup
		return
	}

	ok, _ := kv.hasExecuted(args.Client, args.SerialNumber, args.Key)
	if ok {
		DPrintf(kv.me, "PutAppend hasExecuted args = %+v\n", args)
		reply.Err = OK
		return
	}

	if !kv.canExecute(args.Client, args.SerialNumber, args.Key) {
		DPrintf(kv.me, "PutAppend !canExecute args = %+v\n", args)
		reply.Err = ErrWrongOrder
		return
	}

	op := Op{args.Op, args.Key, args.Value, args.Client, args.SerialNumber}
	err := kv.startAndWaitForOp(op)
	if err != OK {
		reply.Err = err
		return
	} else if !kv.keyInGroup(args.Key) {
		reply.Err = ErrWrongGroup
		return
	}

	reply.Err = OK
	DPrintf(kv.me, "successful putappend with %+v\n", args)
}

func (kv *ShardKV) hasExecuted(client int64, serialNumber int, key string) (bool, string) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if kv.ClientKeySerialNumber[client] == nil {
		kv.ClientKeySerialNumber[client] = make(map[string]int)
	}

	executed := false
	var val string
	if kv.ClientKeySerialNumber[client][key] >= serialNumber {
		executed = true
		val = kv.KeyValues[key]
	}

	return executed, val
}

func (kv *ShardKV) canExecute(client int64, serialNumber int, key string) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	return kv.ClientKeySerialNumber[client][key] == serialNumber-1
}

func (kv *ShardKV) keyInGroup(key string) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	shard := key2shard(key)
	gid := kv.config.Shards[shard]

	return gid == kv.gid
}

func (kv *ShardKV) startAndWaitForOp(op Op) Err {
	ok, index := kv.startAndAddWait(op)
	if !ok {
		return ErrWrongLeader
	}

	DPrintf(kv.me, "start op is %+v \n", op)

	for !kv.killed() {
		if kv.checkIndex(index, op) {
			return OK
		}

		if !kv.isLeader() {
			DPrintf(kv.me, "not a leader %+v \n", op)
			return ErrWrongLeader
		}

		if !kv.keyInGroup(op.Key) {
			DPrintf(kv.me, "reconfig %+v \n", op)
			return ErrWrongGroup
		}

		time.Sleep(2 * time.Millisecond)
	}

	return ErrWrongLeader
}

func (kv *ShardKV) startAndAddWait(op Op) (bool, int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// here we make start and set required logs atomic
	// in case the command is commited too fast after we start and before we add watch to it
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		return false, index
	}

	kv.Requiredlogs[index] = 1
	return true, index
}

func (kv *ShardKV) isLeader() bool {
	_, isLeader := kv.rf.GetState()
	return isLeader
}

func (kv *ShardKV) checkIndex(index int, command interface{}) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	appliedCommand, ok := kv.Appliedlogs[index]
	if ok {
		delete(kv.Appliedlogs, index)
		delete(kv.Requiredlogs, index)
	}

	// if we recieve a different command at this index, we will find correspond
	// rf server is no longer a leader and hence return false in startOp
	return ok && appliedCommand == command
}

func (kv *ShardKV) readFromApplyCh() {
	for !kv.killed() {
		for msg := range kv.applyCh {
			kv.mu.Lock()
			if msg.CommandValid {
				kv.dealWithCommandNL(msg.CommandIndex, msg.Command)
			} else if msg.SnapshotValid {
				kv.dealWithSnapShotNL(msg.Snapshot, msg.SnapshotIndex)
			}

			kv.mu.Unlock()
		}
	}
}

func (kv *ShardKV) dealWithCommandNL(commandIndex int, command interface{}) {

	kv.CommitIndex = commandIndex
	DPrintf(kv.me, "command is %+v with index = %d\n", command, commandIndex)

	_, ok := kv.Requiredlogs[commandIndex]
	if ok {
		kv.Appliedlogs[commandIndex] = command
	}

	// persist putandappend result
	op, ok := command.(Op)
	if ok && op.OpType != "" {
		if kv.ClientKeySerialNumber[op.Client] == nil {
			kv.ClientKeySerialNumber[op.Client] = make(map[string]int)
		}

		serialNumber, ok := kv.ClientKeySerialNumber[op.Client][op.Key]
		if ok && serialNumber >= op.SerialNumber {
			DPrintf(kv.me, "has dealt %d", op.SerialNumber)
			return
		}

		if op.OpType == "Put" {
			kv.PutValNL(op.Key, op.Value)
		} else if op.OpType == "Append" {
			kv.AppendValNL(op.Key, op.Value)
		}

		kv.ClientKeySerialNumber[op.Client][op.Key] = op.SerialNumber
	}
}

func (kv *ShardKV) PutValNL(key string, val string) {
	kv.KeyValues[key] = val
}

func (kv *ShardKV) AppendValNL(key string, val string) {
	_, ok := kv.KeyValues[key]
	if !ok {
		kv.PutValNL(key, val)
		return
	}

	kv.KeyValues[key] += val
}

func (kv *ShardKV) dealWithSnapShotNL(snapshot []byte, snapshotIndex int) {
	DPrintf(kv.me, "receiev snapshot is %+v with index %d\n", snapshot, snapshotIndex)

	kv.readSnapShotNL(snapshot)
	kv.CommitIndex = snapshotIndex
}

func (kv *ShardKV) checkLeader() {
	for !kv.killed() {
		kv.mu.Lock()
		if kv.isLeader() && !kv.checkedLeader {
			kv.rf.Start(Op{})
		}
		kv.checkedLeader = kv.isLeader()
		kv.mu.Unlock()

		time.Sleep(10 * time.Millisecond)
	}
}

func (kv *ShardKV) checkSnapshot() {
	for !kv.killed() {

		if kv.persister.RaftStateSize() >= kv.maxraftstate*9/10 {
			kv.makeSnapshot()
		}

		time.Sleep(10 * time.Millisecond)
	}
}

func (kv *ShardKV) makeSnapshot() {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.Appliedlogs)
	e.Encode(kv.Requiredlogs)
	e.Encode(kv.ClientKeySerialNumber)
	e.Encode(kv.CommitIndex)
	e.Encode(kv.KeyValues)
	DPrintf(kv.me, "make snapshot from %d", kv.CommitIndex)
	kv.rf.Snapshot(kv.CommitIndex, w.Bytes())
}

func (kv *ShardKV) readSnapShotNL(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var appliedlogs map[int]interface{}
	var requiredlogs map[int]int
	var clientKeySerialNumber map[int64]map[string]int
	var commitIndex int
	var keyValues map[string]string
	if d.Decode(&appliedlogs) != nil ||
		d.Decode(&requiredlogs) != nil ||
		d.Decode(&clientKeySerialNumber) != nil ||
		d.Decode(&commitIndex) != nil ||
		d.Decode(&keyValues) != nil {

		// todo should we trigger any error here
		return
	} else {
		kv.Appliedlogs = appliedlogs
		kv.Requiredlogs = requiredlogs
		kv.ClientKeySerialNumber = clientKeySerialNumber
		kv.CommitIndex = commitIndex
		kv.KeyValues = keyValues
	}
}

func (kv *ShardKV) restoreFromSnapshot() {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// todo atomic or double check
	kv.readSnapShotNL(kv.persister.ReadSnapshot())
	DPrintf(kv.me, "recovered snapshot is %+v", kv.KeyValues)
}

func (kv *ShardKV) checkConfig() {
	for !kv.killed() {
		oldNumber := kv.config.Num

		kv.mu.Lock()
		kv.config = kv.sm.Query(-1)
		kv.mu.Unlock()

		if oldNumber != kv.config.Num {
			DPrintf(kv.me, "new config is %+v", kv.config)
		}

		time.Sleep(100 * time.Millisecond)
	}
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
	atomic.StoreInt32(&kv.dead, 1)
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers
	kv.persister = persister
	kv.sm = shardctrler.MakeClerk(ctrlers)

	// Your initialization code here.
	kv.KeyValues = make(map[string]string)
	kv.Appliedlogs = make(map[int]interface{})
	kv.Requiredlogs = make(map[int]int)
	kv.ClientKeySerialNumber = make(map[int64]map[string]int)
	kv.restoreFromSnapshot()

	// Use something like this to talk to the shardctrler:
	// kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	go kv.readFromApplyCh()

	go kv.checkLeader()

	if kv.maxraftstate != -1 {
		go kv.checkSnapshot()
	}

	go kv.checkConfig()

	return kv
}
