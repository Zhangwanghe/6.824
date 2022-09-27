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
	shardConfigNumber     map[int]int
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	if !kv.keyInGroup(args.Key) {
		DPrintf(kv.gid, kv.me, "Get !keyInGroup args = %+v\n", args)
		reply.Err = ErrWrongGroup
		return
	}

	if kv.isReconfiguring(args.Key) {
		DPrintf(kv.gid, kv.me, "Get isReconfiguring args = %+v\n", args)
		reply.Err = ErrWrongOrder
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

	if kv.isReconfiguring(args.Key) {
		DPrintf(kv.gid, kv.me, "PutAppend isReconfiguring args = %+v\n", args)
		reply.Err = ErrWrongOrder
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

func (kv *ShardKV) isReconfiguring(key string) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	return kv.shardConfigNumber[key2shard(key)] != kv.configs[len(kv.configs)-1].Num
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

	// deal with ctrl info for configuration
	ctrl, ok := command.(Ctrl)
	if ok {
		if ctrl.CtrlType == "Reconfig" {
			kv.reconfigNL(ctrl.Config)
		} else if ctrl.CtrlType == "MoveShards" {
			kv.moveShardsNL(ctrl.Shard, ctrl.MoveShardReply)
		} else if ctrl.CtrlType == "RemoveOldConfig" {
			kv.removeOldConfigNL(ctrl.LastConfigNumber)
		} else if ctrl.CtrlType == "RemoveShard" {
			kv.removeShardNL(ctrl.LastConfigNumber, ctrl.Config.Shards)
		}
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

func (kv *ShardKV) reconfigNL(config shardctrler.Config) {
	DPrintf(kv.gid, kv.me, "append config is %d\n", config)
	kv.configs = append(kv.configs, config)

	if config.Num > 1 {
		return
	}

	for i := 0; i < shardctrler.NShards; i++ {
		kv.shardConfigNumber[i] = 1
	}
}

func (kv *ShardKV) moveShardsNL(shard int, reply MoveShardsReply) {
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

	kv.updateShardConfigNumberNL(shard, reply.ShardConfigNumber)
}

func (kv *ShardKV) updateShardConfigNumberNL(shard int, number int) {
	kv.shardConfigNumber[shard] = raft.Max(kv.shardConfigNumber[shard], number)
	DPrintf(kv.gid, kv.me, "update shard %d to %d\n", shard, number)
}

func (kv *ShardKV) removeOldConfigNL(configNumber int) {
	if len(kv.configs) == 0 || kv.configs[0].Num != configNumber {
		return
	}

	DPrintf(kv.gid, kv.me, "remove config %+v\n", kv.configs[0])
	kv.configs = kv.configs[1:]
}

func (kv *ShardKV) removeShardNL(configNumber int, shards [shardctrler.NShards]int) {
	if len(kv.configs) != 1 || configNumber != kv.configs[0].Num {
		DPrintf(kv.gid, kv.me, "removeShardNL kv.configs = %+v \n", kv.configs)
		return
	}

	DPrintf(kv.gid, kv.me, "removeShardNL %+v for %+v \n", shards, kv.configs[0])

	for key, _ := range kv.KeyValues {
		if shards[key2shard(key)] == 1 {
			delete(kv.KeyValues, key)
		}
	}

	for client, keySerial := range kv.ClientKeySerialNumber {
		for key, _ := range keySerial {
			if shards[key2shard(key)] == 1 {
				delete(kv.ClientKeySerialNumber[client], key)
			}
		}
	}

	DPrintf(kv.gid, kv.me, "after removeShardNL kv.KeyValues = %+v kv.ClientKeySerialNumber = %+v \n", kv.KeyValues, kv.ClientKeySerialNumber)
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
			kv.mu.Unlock()

			index, _, _ := kv.rf.Start(Op{})
			kv.waitForStartIndex(index)

			kv.mu.Lock()
			if kv.isLeader() {
				go kv.checkConfig()
				go kv.checkConfigDiff()
				go kv.checkDeleteUselessShard()
			}
		}
		kv.checkedLeader = kv.isLeader()
		kv.mu.Unlock()

		time.Sleep(10 * time.Millisecond)
	}
}

func (kv *ShardKV) waitForStartIndex(index int) {
	DPrintf(kv.gid, kv.me, "converting to leader and waitForStartIndex %d \n", index)
	for !kv.killed() {
		kv.mu.Lock()
		commitIndex := kv.CommitIndex
		kv.mu.Unlock()

		if commitIndex >= index {
			break
		}

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
	e.Encode(kv.configs)
	e.Encode(kv.shardConfigNumber)
	//DPrintf(kv.gid, kv.me, "make snapshot from %d with kv.KeyValues = %+v kv.shardConfigNumber = %+v \n", kv.CommitIndex, kv.KeyValues, kv.shardConfigNumber)
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
	var configs []shardctrler.Config
	var shardConfigNumber map[int]int
	if d.Decode(&appliedlogs) != nil ||
		d.Decode(&requiredlogs) != nil ||
		d.Decode(&clientKeySerialNumber) != nil ||
		d.Decode(&commitIndex) != nil ||
		d.Decode(&keyValues) != nil ||
		d.Decode(&configs) != nil ||
		d.Decode(&shardConfigNumber) != nil {
		return
	} else {
		kv.Appliedlogs = appliedlogs
		kv.Requiredlogs = requiredlogs
		kv.ClientKeySerialNumber = clientKeySerialNumber
		kv.CommitIndex = commitIndex
		kv.KeyValues = keyValues
		kv.configs = configs
		kv.shardConfigNumber = shardConfigNumber
	}
}

func (kv *ShardKV) restoreFromSnapshot() {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	kv.readSnapShotNL(kv.persister.ReadSnapshot())
	DPrintf(kv.gid, kv.me, "recovered snapshot is %+v and kv.shardConfigNumber = %+v\n", kv.KeyValues, kv.shardConfigNumber)
}

func (kv *ShardKV) checkConfig() {
	for !kv.killed() && kv.isLeader() {
		latestConfigNumber := kv.getLatestConfigNumber()
		config := kv.sm.Query(-1)
		for i := latestConfigNumber + 1; i <= config.Num; i++ {
			kv.rf.Start(kv.makeReconfigCtrlNL(kv.sm.Query(i)))
		}

		if config.Num > latestConfigNumber {
			kv.waitForAppendingConfigNumber(config.Num)
		}

		time.Sleep(100 * time.Millisecond)
	}
}

func (kv *ShardKV) getLatestConfigNumber() int {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if len(kv.configs) > 0 {
		return kv.configs[len(kv.configs)-1].Num
	}

	return 0
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

func (kv *ShardKV) waitForAppendingConfigNumber(number int) {
	DPrintf(kv.gid, kv.me, "wait for config %d to be appended\n", number)

	for !kv.killed() {
		latestConfigNumber := kv.getLatestConfigNumber()

		if number == latestConfigNumber {
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
	ShardConfigNumber     int
}

func (kv *ShardKV) MoveShards(args *MoveShardsArgs, reply *MoveShardsReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	DPrintf(kv.gid, kv.me, "MoveShards args is %+v\n", args)
	if !kv.isLeader() || !kv.shouldMovingShardsForConfigNL(args.Shard, args.LastConfigNumber) {
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

	reply.ShardConfigNumber = kv.shardConfigNumber[args.Shard]

	DPrintf(kv.gid, kv.me, "MoveShards reply is %+v\n", reply)
}

func (kv *ShardKV) shouldMovingShardsForConfigNL(shard int, configNumber int) bool {
	// when partition we should response for duplicated requests
	return len(kv.configs) > 0 && kv.shardConfigNumber[shard] > configNumber
}

func (kv *ShardKV) checkConfigDiff() {
	for i := 0; i < shardctrler.NShards; i++ {
		go func(shard int) {
			for !kv.killed() && kv.isLeader() {
				if kv.shouldAskForMoveShards(shard) {
					if kv.checkInitialConfig(shard) {
						continue
					}

					kv.askForMoveShardsAndWait(shard)
					if ok, number := kv.shouldRemoveOldConfig(); ok {
						kv.startAndWaitForRemoveOldConfig(number)
					}
				} else {
					time.Sleep(10 * time.Millisecond)
				}
			}
		}(i)
	}
}

func (kv *ShardKV) checkInitialConfig(shard int) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	return kv.shardConfigNumber[shard] == 0
}

func (kv *ShardKV) shouldAskForMoveShards(shard int) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	return len(kv.configs) > 0 && kv.shardConfigNumber[shard] < kv.configs[len(kv.configs)-1].Num
}

func (kv *ShardKV) askForMoveShardsAndWait(shard int) {
	DPrintf(kv.gid, kv.me, "askForMoveShardsAndWait %d \n", shard)
	oldConfig, newConfig := kv.findNextConfigForShard(shard)
	if !kv.needMoveShard(shard, oldConfig, newConfig) {
		var reply MoveShardsReply
		reply.ShardConfigNumber = newConfig.Num
		kv.rf.Start(kv.makeMoveShardsCtrl(shard, reply))
	} else {
		args := kv.makeMoveShardArgs(oldConfig.Num, shard)

		timer := time.NewTimer(100 * time.Millisecond)
		defer timer.Stop()
		ch := make(chan int)
		for !kv.killed() {
			if servers, ok := oldConfig.Groups[oldConfig.Shards[args.Shard]]; ok {
				for si := 0; si < len(servers); si++ {
					srv := kv.make_end(servers[si])
					go func(srv *labrpc.ClientEnd, args MoveShardsArgs) {
						var reply MoveShardsReply
						DPrintf(kv.gid, kv.me, "sendReconfigInfoNL args is %+v\n", args)
						ok := srv.Call("ShardKV.MoveShards", &args, &reply)
						DPrintf(kv.gid, kv.me, "sendReconfigInfoNL args is %+v reply is %+v \n", args, reply)
						if ok && reply.Succeed && kv.isConfiguringWith(args.Shard, args.LastConfigNumber) {
							kv.rf.Start(kv.makeMoveShardsCtrl(args.Shard, reply))
							ch <- args.Shard
						}
					}(srv, args)
				}
			}

			timer.Stop()
			timer.Reset(100 * time.Millisecond)

			success := false
			select {
			case <-ch:
				{
					success = true
					// todo close chan
				}
			case <-timer.C:
				{
					break
				}
			}

			if success {
				break
			}
		}
	}

	kv.waitForRemovingOldShard(shard, newConfig.Num)
}

func (kv *ShardKV) findNextConfigForShard(shard int) (shardctrler.Config, shardctrler.Config) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	index := -1
	for i, config := range kv.configs {
		if config.Num > kv.shardConfigNumber[shard] {
			index = i
			break
		}
	}

	return kv.configs[index-1], kv.configs[index]
}

func (kv *ShardKV) needMoveShard(shard int, oldConfig shardctrler.Config, newConfig shardctrler.Config) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	return oldConfig.Shards[shard] != kv.gid && newConfig.Shards[shard] == kv.gid
}

func (kv *ShardKV) isConfiguringWith(shard int, configNumber int) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	return kv.shardConfigNumber[shard] == configNumber
}

func (kv *ShardKV) makeMoveShardsCtrl(shard int, reply MoveShardsReply) Ctrl {
	ctrl := Ctrl{}
	ctrl.CtrlType = "MoveShards"
	ctrl.MoveShardReply = reply
	ctrl.Shard = shard

	return ctrl
}

func (kv *ShardKV) makeMoveShardArgs(configNumber int, shard int) MoveShardsArgs {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	var args MoveShardsArgs
	args.LastConfigNumber = configNumber
	args.Shard = shard
	return args
}

func (kv *ShardKV) waitForRemovingOldShard(shard int, configNumber int) {
	for !kv.killed() {
		kv.mu.Lock()
		number := kv.shardConfigNumber[shard]
		kv.mu.Unlock()

		if number >= configNumber {
			break
		}

		time.Sleep(10 * time.Millisecond)
	}
}

func (kv *ShardKV) shouldRemoveOldConfig() (bool, int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	minConfig := 1000000000
	for _, configNumber := range kv.shardConfigNumber {
		minConfig = raft.Min(minConfig, configNumber)
	}

	return minConfig > kv.configs[0].Num, kv.configs[0].Num
}

func (kv *ShardKV) startAndWaitForRemoveOldConfig(configNumber int) {
	if kv.isRemoved(configNumber) {
		return
	}

	DPrintf(kv.gid, kv.me, "startAndWaitForRemoveOldConfig %+v\n", configNumber)

	var ctrl Ctrl
	ctrl.CtrlType = "RemoveOldConfig"
	ctrl.LastConfigNumber = configNumber
	index, _, _ := kv.rf.Start(ctrl)
	kv.waitForStartIndex(index)
}

func (kv *ShardKV) isRemoved(configNumber int) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	return len(kv.configs) > 0 && kv.configs[0].Num != configNumber
}

type GetShardConfigNumberArgs struct {
	Shard int
}

type GetShardConfigNumberReply struct {
	ConfigNumber int
}

func (kv *ShardKV) GetShardConfigNumber(args *GetShardConfigNumberArgs, reply *GetShardConfigNumberReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	reply.ConfigNumber = kv.shardConfigNumber[args.Shard]
	DPrintf(kv.gid, kv.me, "GetShardConfigNumber args = %+v, reply = %+v\n", args, reply)
}

func (kv *ShardKV) checkDeleteUselessShard() {
	for !kv.killed() && kv.isLeader() {
		if shards := kv.getUselessShard(); len(shards) > 0 {
			kv.deleteUselessShard(kv.configs[0], shards)
		} else {
			time.Sleep(10 * time.Millisecond)
		}
	}
}

func (kv *ShardKV) getUselessShard() map[int]int {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	shards := make(map[int]int)

	if len(kv.configs) == 1 {
		for k, _ := range kv.KeyValues {
			if kv.configs[0].Shards[key2shard(k)] != kv.gid {
				shards[key2shard(k)] = 1
			}
		}

		for _, ks := range kv.ClientKeySerialNumber {
			for k, _ := range ks {
				if kv.configs[0].Shards[key2shard(k)] != kv.gid {
					shards[key2shard(k)] = 1
				}
			}
		}
	}

	return shards
}

func (kv *ShardKV) deleteUselessShard(config shardctrler.Config, shards map[int]int) {
	DPrintf(kv.gid, kv.me, "deleteUselessShard %+v for %+v \n", shards, config)

	for !kv.killed() && kv.getLatestConfigNumber() == config.Num {
		timer := time.NewTimer(100 * time.Millisecond)
		defer timer.Stop()
		waitNum := len(shards)
		bch := make(chan int, shardctrler.NShards)

		for shard, _ := range shards {
			go func(shard int) {
				ch := make(chan int)
				args := GetShardConfigNumberArgs{shard}

				if servers, ok := config.Groups[config.Shards[shard]]; ok {
					for si := 0; si < len(servers); si++ {
						srv := kv.make_end(servers[si])
						go func(srv *labrpc.ClientEnd) {
							var reply GetShardConfigNumberReply
							DPrintf(kv.gid, kv.me, "checkDeleteUselessKey args is %+v\n", args)
							ok := srv.Call("ShardKV.GetShardConfigNumber", &args, &reply)
							DPrintf(kv.gid, kv.me, "checkDeleteUselessKey args is %+v reply is %+v \n", args, reply)
							if ok && reply.ConfigNumber == config.Num {
								ch <- args.Shard
							}
						}(srv)
					}
				}

				<-ch
				bch <- shard
			}(shard)
		}

		if waitNum != 0 {
			timer.Stop()
			timer.Reset(100 * time.Millisecond)

			timeout := false
			for !kv.killed() && !timeout && waitNum > 0 {
				select {
				case <-bch:
					{
						waitNum--
					}
				case <-timer.C:
					{
						timeout = true
					}
				}
			}
		}

		if waitNum == 0 {
			index, _, _ := kv.rf.Start(kv.makeRemoveShardArgs(config.Num, shards))
			kv.waitForStartIndex(index)
			break
		}
	}

	for !kv.killed() && kv.getLatestConfigNumber() == config.Num {
		time.Sleep(10 * time.Millisecond)
	}
}

func (kv *ShardKV) makeRemoveShardArgs(configNumber int, shards map[int]int) Ctrl {
	ctrl := Ctrl{}
	ctrl.CtrlType = "RemoveShard"
	for shard, _ := range shards {
		ctrl.Config.Shards[shard] = 1
	}
	ctrl.LastConfigNumber = configNumber
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
	kv.shardConfigNumber = make(map[int]int)
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

	return kv
}
