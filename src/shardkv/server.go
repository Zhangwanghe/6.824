package shardkv

import (
	"bytes"
	"fmt"
	"log"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"6.824/shardctrler"
)

const Debug = false

func DPrintf(group int, index int, format string, a ...interface{}) (n int, err error) {
	if Debug {
		time := time.Now()
		timeFormat := "2006-01-02 15:04:05.000"
		prefix := fmt.Sprintf("%s G%d-S%d ", time.Format(timeFormat), group, index)
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

type Ctrl struct {
	CtrlType         string
	Config           shardctrler.Config
	Shard            int
	MoveShardReply   MoveShardsReply
	LastConfigNumber int
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
	configs               []shardctrler.Config

	waitForMovingShardsRequest map[int]int
	waitForMovingShardsReply   map[int]int
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	if !kv.keyInGroup(args.Key) {
		DPrintf(kv.gid, kv.me, "Get !keyInGroup args = %+v\n", args)
		reply.Err = ErrWrongGroup
		return
	}

	if kv.isReconfiguring() {
		DPrintf(kv.gid, kv.me, "Get isReconfiguring args = %+v\n", args)
		reply.Err = ErrNoKey
		return
	}

	ok, val := kv.hasExecuted(args.Client, args.SerialNumber, args.Key)
	if ok {
		DPrintf(kv.gid, kv.me, "Get hasExecuted\n")
		reply.Value = val
		reply.Err = OK
		return
	}

	if !kv.canExecute(args.Client, args.SerialNumber, args.Key) {
		DPrintf(kv.gid, kv.me, "Get !canExecute args = %+v\n", args)
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
		DPrintf(kv.gid, kv.me, "PutAppend !keyInGroup args = %+v\n", args)
		reply.Err = ErrWrongGroup
		return
	}

	if kv.isReconfiguring() {
		DPrintf(kv.gid, kv.me, "PutAppend isReconfiguring args = %+v\n", args)
		reply.Err = ErrNoKey
		return
	}

	ok, _ := kv.hasExecuted(args.Client, args.SerialNumber, args.Key)
	if ok {
		DPrintf(kv.gid, kv.me, "PutAppend hasExecuted args = %+v\n", args)
		reply.Err = OK
		return
	}

	if !kv.canExecute(args.Client, args.SerialNumber, args.Key) {
		DPrintf(kv.gid, kv.me, "PutAppend !canExecute args = %+v\n", args)
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
	DPrintf(kv.gid, kv.me, "successful putappend with %+v\n", args)
}

func (kv *ShardKV) hasExecuted(client int64, serialNumber int, key string) (bool, string) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	return kv.hasExecutedNL(client, serialNumber, key)
}

func (kv *ShardKV) hasExecutedNL(client int64, serialNumber int, key string) (bool, string) {
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

	return kv.keyInGroupNL(key)
}

func (kv *ShardKV) keyInGroupNL(key string) bool {
	if len(kv.configs) == 0 {
		return false
	}

	shard := key2shard(key)
	gid := kv.configs[len(kv.configs)-1].Shards[shard]

	return gid == kv.gid
}

func (kv *ShardKV) isReconfiguring() bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	return len(kv.configs) > 1
}

func (kv *ShardKV) startAndWaitForOp(op Op) Err {
	ok, index := kv.startAndAddWait(op)
	if !ok {
		return ErrWrongLeader
	}

	DPrintf(kv.gid, kv.me, "start op is %+v \n", op)

	for !kv.killed() {
		if kv.checkIndex(index, op) {
			return OK
		}

		if !kv.isLeader() {
			DPrintf(kv.gid, kv.me, "not a leader %+v \n", op)
			return ErrWrongLeader
		}

		if !kv.keyInGroup(op.Key) {
			DPrintf(kv.gid, kv.me, "!kv.keyInGroup due to reconfig %+v \n", op)
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
	return ok && reflect.DeepEqual(appliedCommand, command)
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
	DPrintf(kv.gid, kv.me, "command is %+v with index = %d\n", command, commandIndex)

	_, ok := kv.Requiredlogs[commandIndex]
	if ok {
		kv.Appliedlogs[commandIndex] = command
	}

	// persist putandappend result
	op, ok := command.(Op)
	if ok && op.OpType != "" {
		if ok, _ = kv.hasExecutedNL(op.Client, op.SerialNumber, op.Key); ok {
			DPrintf(kv.gid, kv.me, "has dealt %d\n", op.SerialNumber)
			return
		}

		if !kv.keyInGroupNL(op.Key) {
			DPrintf(kv.gid, kv.me, "!kv.keyInGroup due to reconfig when commiting %+v \n", op)
			return
		}

		if op.OpType == "Put" {
			kv.PutValNL(op.Key, op.Value)
		} else if op.OpType == "Append" {
			kv.AppendValNL(op.Key, op.Value)
		}

		kv.ClientKeySerialNumber[op.Client][op.Key] = op.SerialNumber
	}

	ctrl, ok := command.(Ctrl)
	if ok {
		if ctrl.CtrlType == "Reconfig" {
			DPrintf(kv.gid, kv.me, "append config is %d\n", ctrl.Config)
			kv.configs = append(kv.configs, ctrl.Config)
		} else if ctrl.CtrlType == "MoveShards" {
			kv.moveShardsNL(ctrl.Shard, ctrl.LastConfigNumber, ctrl.MoveShardReply)
		} else if ctrl.CtrlType == "RemoveOldConfig" {
			kv.removeOldConfigNL(ctrl.LastConfigNumber)
		}
	}
}

func (kv *ShardKV) moveShardsNL(shard int, configNumber int, reply MoveShardsReply) {
	DPrintf(kv.gid, kv.me, "moveShardsNL %+v with reply %+v\n", shard, reply)
	for k, v := range reply.KeyValue {
		kv.PutValNL(k, v)
	}

	for c, ks := range reply.ClientKeySerialNumber {
		for k, s := range ks {
			kv.hasExecutedNL(c, s, k)
			kv.ClientKeySerialNumber[c][k] = s
		}
	}

	delete(kv.waitForMovingShardsReply, shard)

	if kv.isLeader() {
		go kv.synchronizeRemoveOldConfig(configNumber)
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

func (kv *ShardKV) removeOldConfigNL(configNumber int) {
	if len(kv.configs) == 0 || kv.configs[0].Num != configNumber {
		return
	}

	DPrintf(kv.gid, kv.me, "remove config %+v\n", kv.configs[0])
	kv.configs = kv.configs[1:]
}

func (kv *ShardKV) dealWithSnapShotNL(snapshot []byte, snapshotIndex int) {
	DPrintf(kv.gid, kv.me, "receiev snapshot is %+v with index %d\n", snapshot, snapshotIndex)

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
	DPrintf(kv.gid, kv.me, "make snapshot from %d", kv.CommitIndex)
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

	kv.readSnapShotNL(kv.persister.ReadSnapshot())
	DPrintf(kv.gid, kv.me, "recovered snapshot is %+v", kv.KeyValues)
}

func (kv *ShardKV) checkConfig() {
	for !kv.killed() {
		if kv.isLeader() {
			kv.mu.Lock()
			oldNumber := 0
			if len(kv.configs) > 0 {
				oldNumber = kv.configs[len(kv.configs)-1].Num
			}
			kv.mu.Unlock()

			config := kv.sm.Query(-1)
			for i := oldNumber + 1; i <= config.Num; i++ {
				kv.rf.Start(kv.makeReconfigCtrlNL(kv.sm.Query(i)))
			}

			if config.Num > oldNumber {
				kv.waitForConfigNumber(config.Num)
			}
		}

		time.Sleep(100 * time.Millisecond)
	}
}

func (kv *ShardKV) makeReconfigCtrlNL(config shardctrler.Config) Ctrl {
	ctrl := Ctrl{}
	ctrl.CtrlType = "Reconfig"

	ctrl.Config = shardctrler.Config{}
	ctrl.Config.Num = config.Num
	for i := 0; i < shardctrler.NShards; i++ {
		ctrl.Config.Shards[i] = config.Shards[i]
	}

	ctrl.Config.Groups = make(map[int][]string)
	for k, v := range config.Groups {
		ctrl.Config.Groups[k] = make([]string, len(v))
		copy(ctrl.Config.Groups[k], v)
	}

	return ctrl
}

func (kv *ShardKV) waitForConfigNumber(number int) {
	DPrintf(kv.gid, kv.me, "wait for config %d to be appended\n", number)

	for {
		kv.mu.Lock()
		newConfigNumber := 0
		if len(kv.configs) > 0 {
			newConfigNumber = kv.configs[len(kv.configs)-1].Num
		}
		kv.mu.Unlock()

		if number == newConfigNumber {
			break
		}

		time.Sleep(10 * time.Millisecond)
	}
}

type MoveShardsArgs struct {
	LastConfigNumber int
	Shard            int
}

type MoveShardsReply struct {
	Succeed               bool
	KeyValue              map[string]string
	ClientKeySerialNumber map[int64]map[string]int
}

func (kv *ShardKV) MoveShards(args *MoveShardsArgs, reply *MoveShardsReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	DPrintf(kv.gid, kv.me, "MoveShards args is %+v\n", args)
	// todo when having partition, how to ensure we are contacting correct leader?
	if !kv.isLeader() || !kv.hasConvertedToConfigNL(args.LastConfigNumber) {
		reply.Succeed = false
		return
	}

	reply.Succeed = true

	reply.KeyValue = make(map[string]string)
	for k, v := range kv.KeyValues {
		if key2shard(k) == args.Shard {
			reply.KeyValue[k] = v
		}
	}

	reply.ClientKeySerialNumber = make(map[int64]map[string]int)
	for c, ks := range kv.ClientKeySerialNumber {
		for k, s := range ks {
			if _, ok := reply.KeyValue[k]; ok {
				if reply.ClientKeySerialNumber[c] == nil {
					reply.ClientKeySerialNumber[c] = make(map[string]int)
				}

				reply.ClientKeySerialNumber[c][k] = s
			}
		}
	}

	delete(kv.waitForMovingShardsRequest, args.Shard)
	go kv.synchronizeRemoveOldConfig(args.LastConfigNumber)

	DPrintf(kv.gid, kv.me, "MoveShards reply is %+v\n", reply)
}

func (kv *ShardKV) hasConvertedToConfigNL(configNumber int) bool {
	return len(kv.configs) > 0 && kv.configs[0].Num >= configNumber
}

func (kv *ShardKV) synchronizeRemoveOldConfig(configNumber int) {
	if !kv.hasFinishedOneReconfig() {
		return
	}

	if !kv.isRemoved(configNumber) {
		return
	}

	DPrintf(kv.gid, kv.me, "synchronizeRemoveOldConfig %+v\n", configNumber)

	var ctrl Ctrl
	ctrl.CtrlType = "RemoveOldConfig"
	ctrl.LastConfigNumber = configNumber
	kv.rf.Start(ctrl)

	kv.waitForRemovingOldConfig(configNumber)
}

func (kv *ShardKV) isRemoved(configNumber int) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	return len(kv.configs) > 0 && kv.configs[0].Num != configNumber
}

func (kv *ShardKV) waitForRemovingOldConfig(number int) {
	for {
		kv.mu.Lock()
		newConfigNumber := 0
		if len(kv.configs) > 0 {
			newConfigNumber = kv.configs[len(kv.configs)-1].Num
		}
		kv.mu.Unlock()

		if number != newConfigNumber {
			break
		}

		time.Sleep(10 * time.Millisecond)
	}
}

func (kv *ShardKV) checkConfigDiff() {
	for !kv.killed() {
		if kv.isLeader() && kv.hasFinishedOneReconfig() {
			if ok, oldConfig, newConifg := kv.getReconfigData(); ok {
				kv.reconfig(oldConfig, newConifg)
				// nothing changes
				// todo add flag for leader waiting for commit
				if kv.hasFinishedOneReconfig() {
					kv.synchronizeRemoveOldConfig(oldConfig.Num)
				}
			}
		}

		time.Sleep(10 * time.Millisecond)
	}
}

func (kv *ShardKV) hasFinishedOneReconfig() bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	return kv.hasFinishedOneReconfigNL()
}

func (kv *ShardKV) hasFinishedOneReconfigNL() bool {
	return len(kv.waitForMovingShardsReply) == 0 && len(kv.waitForMovingShardsRequest) == 0
}

func (kv *ShardKV) getReconfigData() (bool, shardctrler.Config, shardctrler.Config) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if len(kv.configs) <= 1 {
		return false, shardctrler.Config{}, shardctrler.Config{}
	}

	return true, kv.configs[0], kv.configs[1]
}

func (kv *ShardKV) reconfig(oldConfig shardctrler.Config, newConfig shardctrler.Config) {
	DPrintf(kv.gid, kv.me, "oldconfig is %+v new config is %+v", oldConfig, newConfig)

	argsForGroup := make(map[int]MoveShardsArgs)
	for i := 0; i < shardctrler.NShards; i++ {
		if kv.gid == oldConfig.Shards[i] && kv.gid != newConfig.Shards[i] {
			kv.addWaitForMovingShardsRequest(i)
		} else if kv.gid != oldConfig.Shards[i] && kv.gid == newConfig.Shards[i] {
			args := kv.makeMoveShardArgs(oldConfig.Num, i)
			kv.addWaitForMovingShardsReply(i)
			argsForGroup[i] = args
		}
	}

	kv.sendReconfigInfoNL(oldConfig, argsForGroup)
}

func (kv *ShardKV) makeMoveShardArgs(configNumber int, shard int) MoveShardsArgs {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	var args MoveShardsArgs
	args.LastConfigNumber = configNumber
	args.Shard = shard
	return args
}

func (kv *ShardKV) addWaitForMovingShardsRequest(shard int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	kv.waitForMovingShardsRequest[shard] = 1
}

func (kv *ShardKV) addWaitForMovingShardsReply(shard int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	kv.waitForMovingShardsReply[shard] = 1
}

func (kv *ShardKV) sendReconfigInfoNL(config shardctrler.Config, argsForGroup map[int]MoveShardsArgs) {
	timer := time.NewTimer(100 * time.Millisecond)
	defer timer.Stop()

	ch := make(chan int)
	for !kv.killed() && len(argsForGroup) > 0 {
		for _, args := range argsForGroup {
			if servers, ok := config.Groups[config.Shards[args.Shard]]; ok {
				for si := 0; si < len(servers); si++ {
					srv := kv.make_end(servers[si])
					go func(args MoveShardsArgs) {
						var reply MoveShardsReply
						DPrintf(kv.gid, kv.me, "sendReconfigInfoNL args is %+v\n", args)
						ok := srv.Call("ShardKV.MoveShards", &args, &reply)
						DPrintf(kv.gid, kv.me, "sendReconfigInfoNL args is %+v reply is %+v \n", args, reply)
						if ok && reply.Succeed {
							kv.rf.Start(kv.makeMoveShardsCtrl(args, reply))
							ch <- args.Shard
						}
					}(args)
				}
			}
		}

		timer.Stop()
		timer.Reset(100 * time.Millisecond)

		select {
		case shard := <-ch:
			{
				delete(argsForGroup, shard)
			}
		case <-timer.C:
			{
				continue
			}
		}
	}
}

func (kv *ShardKV) makeMoveShardsCtrl(args MoveShardsArgs, reply MoveShardsReply) Ctrl {
	ctrl := Ctrl{}
	ctrl.CtrlType = "MoveShards"
	ctrl.MoveShardReply = reply
	ctrl.Shard = args.Shard
	ctrl.LastConfigNumber = args.LastConfigNumber

	return ctrl
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
	labgob.Register(Ctrl{})
	labgob.Register(MoveShardsReply{})

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
	// todo should snapshot new field in ShardKV?
	kv.waitForMovingShardsRequest = make(map[int]int)
	kv.waitForMovingShardsReply = make(map[int]int)
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

	go kv.checkConfigDiff()

	return kv
}
