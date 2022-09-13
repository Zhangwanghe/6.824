package kvraft

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

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big
	persister    *raft.Persister

	// Your definitions here.
	KeyValues          map[string]string
	Appliedlogs        map[int]interface{}
	Requiredlogs       map[int]int
	ClientSerialNumber map[int64]int
	CommitIndex        int
	checkedLeader      bool
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	ok, val := kv.hasExecuted(args.Client, args.SerialNumber, args.Key)
	if ok {
		DPrintf(kv.me, "Get hasExecuted\n")
		reply.Value = val
		return
	}

	if !kv.canExecute(args.Client, args.SerialNumber) {
		DPrintf(kv.me, "Get !canExecute args = %+v\n", args)
		reply.Err = "wrong order"
		return
	}

	op := Op{"Get", args.Key, "", args.Client, args.SerialNumber}
	if !kv.startAndWaitForOp(op) {
		reply.Err = "wrong leader"
		return
	}

	reply.Value = kv.GetVal(args.Key)
}

func (kv *KVServer) GetVal(key string) string {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	val, ok := kv.KeyValues[key]
	if !ok {
		return ""
	}

	return val
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	ok, _ := kv.hasExecuted(args.Client, args.SerialNumber, args.Key)
	if ok {
		DPrintf(kv.me, "PutAppend hasExecuted\n")
		return
	}

	if !kv.canExecute(args.Client, args.SerialNumber) {
		DPrintf(kv.me, "PutAppend !canExecute args = %+v\n", args)
		reply.Err = "wrong order"
		return
	}

	op := Op{args.Op, args.Key, args.Value, args.Client, args.SerialNumber}
	if !kv.startAndWaitForOp(op) {
		// todo whether to return leaderId if possible
		reply.Err = "wrong leader"
		return
	}

	DPrintf(kv.me, "successful putappend with %+v\n", args)
}

func (kv *KVServer) hasExecuted(client int64, serialNumber int, key string) (bool, string) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	executed := false
	var val string
	if kv.ClientSerialNumber[client] >= serialNumber {
		executed = true
		val = kv.KeyValues[key]
	}

	return executed, val
}

func (kv *KVServer) canExecute(client int64, serialNumber int) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	return kv.ClientSerialNumber[client] == serialNumber-1
}

func (kv *KVServer) startAndWaitForOp(op Op) bool {
	ok, index := kv.startAndAddWait(op)
	if !ok {
		return false
	}

	DPrintf(kv.me, "start op is %+v \n", op)

	for {
		if kv.checkIndex(index, op) {
			return true
		}

		if !kv.isLeader() {
			DPrintf(kv.me, "not a leader %+v \n", op)
			break
		}

		time.Sleep(2 * time.Millisecond)
	}

	return false
}

func (kv *KVServer) startAndAddWait(op Op) (bool, int) {
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

func (kv *KVServer) isLeader() bool {
	_, isLeader := kv.rf.GetState()
	return isLeader
}

func (kv *KVServer) checkIndex(index int, command interface{}) bool {
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

func (kv *KVServer) readFromApplyCh() {
	for !kv.killed() {
		for msg := range kv.applyCh {
			kv.mu.Lock()
			if msg.CommandValid {
				kv.dealWithCommandNL(msg.CommandIndex, msg.Command)
			} else if msg.SnapshotValid {
				kv.dealWithSnapShotNL(msg.Snapshot, msg.SnapshotIndex)
			} else {
				// todo log leaderId
			}
			kv.mu.Unlock()
		}
	}
}

func (kv *KVServer) dealWithCommandNL(commandIndex int, command interface{}) {

	kv.CommitIndex = commandIndex
	DPrintf(kv.me, "command is %+v \n", command)

	_, ok := kv.Requiredlogs[commandIndex]
	if ok {
		kv.Appliedlogs[commandIndex] = command
	}

	// persist putandappend result
	op, ok := command.(Op)
	if ok {
		serialNumber, ok := kv.ClientSerialNumber[op.Client]
		if ok && serialNumber >= op.SerialNumber {
			DPrintf(kv.me, "has dealt %d", op.SerialNumber)
			return
		}

		if op.OpType == "Put" {
			kv.PutValNL(op.Key, op.Value)
		} else if op.OpType == "Append" {
			kv.AppendValNL(op.Key, op.Value)
		}

		kv.ClientSerialNumber[op.Client] = op.SerialNumber
	}
}

func (kv *KVServer) PutValNL(key string, val string) {
	kv.KeyValues[key] = val
}

func (kv *KVServer) AppendValNL(key string, val string) {
	_, ok := kv.KeyValues[key]
	if !ok {
		kv.PutValNL(key, val)
		return
	}

	kv.KeyValues[key] += val
}

func (kv *KVServer) dealWithSnapShotNL(snapshot []byte, snapshotIndex int) {
	DPrintf(kv.me, "receiev snapshot is %+v with index %d\n", snapshot, snapshotIndex)

	kv.readSnapShotNL(snapshot)
	kv.CommitIndex = snapshotIndex
}

func (kv *KVServer) checkLeader() {
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

func (kv *KVServer) checkSnapshot() {
	for !kv.killed() {

		if kv.persister.RaftStateSize() >= kv.maxraftstate*9/10 {
			kv.makeSnapshot()
		}

		time.Sleep(10 * time.Millisecond)
	}
}

func (kv *KVServer) makeSnapshot() {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.Appliedlogs)
	e.Encode(kv.Requiredlogs)
	e.Encode(kv.ClientSerialNumber)
	e.Encode(kv.CommitIndex)
	e.Encode(kv.KeyValues)
	DPrintf(kv.me, "make snapshot from %d", kv.CommitIndex)
	kv.rf.Snapshot(kv.CommitIndex, w.Bytes())
}

func (kv *KVServer) readSnapShotNL(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var appliedlogs map[int]interface{}
	var requiredlogs map[int]int
	var clientSerialNumber map[int64]int
	var commitIndex int
	var keyValues map[string]string
	if d.Decode(&appliedlogs) != nil ||
		d.Decode(&requiredlogs) != nil ||
		d.Decode(&clientSerialNumber) != nil ||
		d.Decode(&commitIndex) != nil ||
		d.Decode(&keyValues) != nil {

		// todo should we trigger any error here
		return
	} else {
		kv.Appliedlogs = appliedlogs
		kv.Requiredlogs = requiredlogs
		kv.ClientSerialNumber = clientSerialNumber
		kv.CommitIndex = commitIndex
		kv.KeyValues = keyValues
	}
}

func (kv *KVServer) restoreFromSnapshot() {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// todo atomic or double check
	kv.readSnapShotNL(kv.persister.ReadSnapshot())
	firstIndex, entries := kv.rf.GetAppliedLogs()
	for index, entry := range entries {
		kv.dealWithCommandNL(firstIndex+index, entry.Command)
	}

	DPrintf(kv.me, "recovered snapshot is %+v", kv.KeyValues)
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})
	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.persister = persister

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.KeyValues = make(map[string]string)
	kv.Appliedlogs = make(map[int]interface{})
	kv.Requiredlogs = make(map[int]int)
	kv.ClientSerialNumber = make(map[(int64)]int)
	kv.restoreFromSnapshot()

	go kv.readFromApplyCh()

	go kv.checkLeader()

	if kv.maxraftstate != -1 {
		go kv.checkSnapshot()
	}

	return kv
}
