package shardctrler

import (
	"fmt"
	"log"
	"reflect"
	"sort"
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

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	dead int32 // set by Kill()

	configs []Config // indexed by config num

	// copy from scraft
	Appliedlogs        map[int]interface{}
	Requiredlogs       map[int]int
	ClientSerialNumber map[int64]int
	CommitIndex        int
	checkedLeader      bool

	groupOrder []int
}

type Op struct {
	// Your data here.
	OpType       string
	Client       int64
	SerialNumber int
	Args         interface{}
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	ok := sc.hasExecuted(args.Client, args.SerialNumber)
	if ok {
		DPrintf(sc.me, "Join hasExecuted\n")
		return
	}

	if !sc.canExecute(args.Client, args.SerialNumber) {
		DPrintf(sc.me, "Join !canExecute args = %+v\n", args)
		reply.WrongLeader = true
		return
	}

	op := Op{"Join", args.Client, args.SerialNumber, *args}
	if !sc.startAndWaitForOp(op) {
		reply.WrongLeader = true
		return
	}

	DPrintf(sc.me, "successful Join with %+v\n", args)
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	ok := sc.hasExecuted(args.Client, args.SerialNumber)
	if ok {
		DPrintf(sc.me, "Leave hasExecuted\n")
		return
	}

	if !sc.canExecute(args.Client, args.SerialNumber) {
		DPrintf(sc.me, "Leave !canExecute args = %+v\n", args)
		reply.WrongLeader = true
		return
	}

	op := Op{"Leave", args.Client, args.SerialNumber, *args}
	if !sc.startAndWaitForOp(op) {
		reply.WrongLeader = true
		return
	}

	DPrintf(sc.me, "successful Leave with %+v\n", args)
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	ok := sc.hasExecuted(args.Client, args.SerialNumber)
	if ok {
		DPrintf(sc.me, "Move hasExecuted\n")
		return
	}

	if !sc.canExecute(args.Client, args.SerialNumber) {
		DPrintf(sc.me, "Move !canExecute args = %+v\n", args)
		reply.WrongLeader = true
		return
	}

	op := Op{"Move", args.Client, args.SerialNumber, *args}
	if !sc.startAndWaitForOp(op) {
		reply.WrongLeader = true
		return
	}

	DPrintf(sc.me, "successful Move with %+v\n", args)
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	ok := sc.hasExecuted(args.Client, args.SerialNumber)
	if ok {
		DPrintf(sc.me, "Query hasExecuted\n")
		reply.Config = sc.QueryConfig(args.SerialNumber)
		return
	}

	if !sc.canExecute(args.Client, args.SerialNumber) {
		DPrintf(sc.me, "Query !canExecute args = %+v\n", args)
		reply.WrongLeader = true
		return
	}

	op := Op{"Query", args.Client, args.SerialNumber, *args}
	if !sc.startAndWaitForOp(op) {
		reply.WrongLeader = true
		return
	}

	reply.Config = sc.QueryConfig(args.Num)

	DPrintf(sc.me, "successful Query with %+v and reply is %+v \n", args, reply)
}

func (sc *ShardCtrler) QueryConfig(configNumber int) Config {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	if configNumber == -1 || configNumber >= len(sc.configs) {
		return sc.configs[len(sc.configs)-1]
	}

	return sc.configs[configNumber]
}

func (sc *ShardCtrler) hasExecuted(client int64, serialNumber int) bool {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	return sc.ClientSerialNumber[client] >= serialNumber
}

func (sc *ShardCtrler) canExecute(client int64, serialNumber int) bool {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	return sc.ClientSerialNumber[client] == serialNumber-1
}

func (sc *ShardCtrler) startAndWaitForOp(op Op) bool {
	ok, index := sc.startAndAddWait(op)
	if !ok {
		return false
	}

	DPrintf(sc.me, "start op is %+v \n", op)

	for !sc.killed() {
		if sc.checkIndex(index, op) {
			return true
		}

		if !sc.isLeader() {
			DPrintf(sc.me, "not a leader %+v \n", op)
			break
		}

		time.Sleep(2 * time.Millisecond)
	}

	return false
}

func (sc *ShardCtrler) startAndAddWait(op Op) (bool, int) {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	// here we make start and set required logs atomic
	// in case the command is commited too fast after we start and before we add watch to it
	index, _, isLeader := sc.rf.Start(op)
	if !isLeader {
		return false, index
	}

	sc.Requiredlogs[index] = 1
	return true, index
}

func (sc *ShardCtrler) isLeader() bool {
	_, isLeader := sc.rf.GetState()
	return isLeader
}

func (sc *ShardCtrler) checkIndex(index int, command interface{}) bool {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	appliedCommand, ok := sc.Appliedlogs[index]
	if ok {
		delete(sc.Appliedlogs, index)
		delete(sc.Requiredlogs, index)
	}

	// if we recieve a different command at this index, we will find correspond
	// rf server is no longer a leader and hence return false in startOp
	return ok && reflect.DeepEqual(appliedCommand, command)
}

func (sc *ShardCtrler) readFromApplyCh() {
	for msg := range sc.applyCh {
		sc.mu.Lock()
		if msg.CommandValid {
			sc.dealWithCommandNL(msg.CommandIndex, msg.Command)
		}
		sc.mu.Unlock()
	}
}

func (sc *ShardCtrler) dealWithCommandNL(commandIndex int, command interface{}) {
	sc.CommitIndex = commandIndex
	DPrintf(sc.me, "command is %+v with index = %d\n", command, commandIndex)

	_, ok := sc.Requiredlogs[commandIndex]
	if ok {
		sc.Appliedlogs[commandIndex] = command
	}

	// persist putandappend result
	op, ok := command.(Op)
	if ok {
		serialNumber, ok := sc.ClientSerialNumber[op.Client]
		if ok && serialNumber >= op.SerialNumber {
			DPrintf(sc.me, "has dealt %d", op.SerialNumber)
			return
		}

		if op.OpType == "Join" {
			args, ok := op.Args.(JoinArgs)
			if ok {
				sc.joinConfigNL(args.Servers)
			}
		} else if op.OpType == "Leave" {
			args, ok := op.Args.(LeaveArgs)
			if ok {
				sc.leaveConfigNL(args.GIDs)
			}
		} else if op.OpType == "Move" {
			args, ok := op.Args.(MoveArgs)
			if ok {
				sc.moveConfigNL(args.Shard, args.GID)
			}
		}

		sc.ClientSerialNumber[op.Client] = op.SerialNumber
	}
}

func (sc *ShardCtrler) joinConfigNL(servers map[int][]string) {
	sc.copyConfigNL()

	for key, value := range servers {
		sc.configs[len(sc.configs)-1].Groups[key] = value
	}

	sc.rebalance(make([]int, 0))
}

func (sc *ShardCtrler) leaveConfigNL(gids []int) {
	sc.copyConfigNL()

	for _, gid := range gids {
		delete(sc.configs[len(sc.configs)-1].Groups, gid)
	}

	sc.rebalance(gids)
}

func (sc *ShardCtrler) moveConfigNL(shard int, gid int) {
	sc.copyConfigNL()
	sc.moveShardNL(shard, gid)
}

func (sc *ShardCtrler) copyConfigNL() {
	config := Config{}
	config.Num = sc.configs[len(sc.configs)-1].Num + 1

	config.Groups = make(map[int][]string)
	for key, value := range sc.configs[len(sc.configs)-1].Groups {
		config.Groups[key] = value
	}

	for i := 0; i < NShards; i++ {
		config.Shards[i] = sc.configs[len(sc.configs)-1].Shards[i]
	}

	sc.configs = append(sc.configs, config)
}

func (sc *ShardCtrler) moveShardNL(shard int, gid int) {
	sc.configs[len(sc.configs)-1].Shards[shard] = gid
}

func (sc *ShardCtrler) rebalance(gids []int) {
	usedGroupId, usedGroupShardNum := sc.getSortedUsedGroupIdAndShardNum()
	restdGroupId, _ := sc.removeLeaveGroup(usedGroupId, usedGroupShardNum, gids)
	unusedGroupId := sc.getSortedUnUsedGroupId(restdGroupId)
	newGroupId := append(restdGroupId, unusedGroupId[:raft.Min(NShards-len(restdGroupId), len(unusedGroupId))]...)
	if len(newGroupId) == 0 {
		newGroupId = append(newGroupId, 0)
	}
	newGroupShardNum := sc.getShardNumForGroups(len(newGroupId))


	oldGroupToShardNumMap := makeMap(usedGroupId, usedGroupShardNum)
	newGroupToShardNumMap := makeMap(newGroupId, newGroupShardNum)

	if len(usedGroupId) == 0 {
		j := 0
		for _, newGid := range newGroupId {
			for k := 0; k < newGroupToShardNumMap[newGid]; k++ {
				sc.moveShardNL(j, newGid)
				j++
			}
		}
		return
	}

	groupShardDiff := diffMap(newGroupToShardNumMap, oldGroupToShardNumMap)

	for _, oldGid := range usedGroupId {
		for groupShardDiff[oldGid] < 0 {
			for _, newGid := range newGroupId {
				for groupShardDiff[newGid] > 0 {
					for i := 0; i < NShards; i++ {
						if sc.configs[len(sc.configs)-1].Shards[i] == oldGid {
							sc.moveShardNL(i, newGid)
							groupShardDiff[newGid]--
							groupShardDiff[oldGid]++
							break
						}
					}

					if groupShardDiff[oldGid] == 0 {
						break
					}
				}

				if groupShardDiff[oldGid] == 0 {
					break
				}
			}
		}
	}
}

func (sc *ShardCtrler) getSortedUsedGroupIdAndShardNum() ([]int, []int) {
	groupToShardNum := make(map[int]int)
	for i := 0; i < NShards; i++ {
		if sc.configs[len(sc.configs)-1].Shards[i] == 0 {
			continue
		}

		groupToShardNum[sc.configs[len(sc.configs)-1].Shards[i]]++
	}

	usedGroupId := make([]int, 0)
	usedGroupShardNum := make([]int, 0)
	for gid, shardNum := range groupToShardNum {
		usedGroupId = append(usedGroupId, gid)
		usedGroupShardNum = append(usedGroupShardNum, shardNum)
	}

	for i := len(usedGroupId) - 1; i >= 1; i-- {
		for j := 0; j < i; j++ {
			if usedGroupShardNum[j] < usedGroupShardNum[j+1] ||
				(usedGroupShardNum[j] == usedGroupShardNum[j+1] && usedGroupId[j] < usedGroupId[j+1]) {
				temp := usedGroupShardNum[j]
				usedGroupShardNum[j] = usedGroupShardNum[j+1]
				usedGroupShardNum[j+1] = temp

				temp = usedGroupId[j]
				usedGroupId[j] = usedGroupId[j+1]
				usedGroupId[j+1] = temp
			}
		}
	}

	return usedGroupId, usedGroupShardNum
}

func (sc *ShardCtrler) removeLeaveGroup(usedGroupId []int, usedGroupShardNum []int, gids []int) ([]int, []int) {
	if len(gids) == 0 {
		return usedGroupId, usedGroupShardNum
	}

	restCount := len(usedGroupId)

	for i := 0; i < len(usedGroupId); i++ {
		for j := 0; j < len(gids); j++ {
			if usedGroupId[i] == gids[j] {
				restCount--
				break
			}
		}
	}

	restGroupId := make([]int, restCount)
	restGroupShardNum := make([]int, restCount)

	index := 0
	for i := 0; i < len(usedGroupId); i++ {
		has := false

		for j := 0; j < len(gids); j++ {
			if usedGroupId[i] == gids[j] {
				has = true
				break
			}
		}

		if !has {
			restGroupId[index] = usedGroupId[i]
			restGroupShardNum[index] = usedGroupShardNum[i]
			index++
		}
	}

	return restGroupId, restGroupShardNum
}

func (sc *ShardCtrler) getSortedUnUsedGroupId(usedGroupId []int) []int {
	unUsedGroupId := make([]int, 0)
	for gid, _ := range sc.configs[len(sc.configs)-1].Groups {
		flag := false

		for i := 0; i < len(usedGroupId); i++ {
			if usedGroupId[i] == gid {
				flag = true
			}
		}

		if !flag {
			unUsedGroupId = append(unUsedGroupId, gid)
		}
	}

	sort.Ints(unUsedGroupId)

	return unUsedGroupId
}

func (sc *ShardCtrler) getShardNumForGroups(groupNum int) []int {
	if groupNum == 0 {
		return make([]int, 0)
	}

	avg := NShards / groupNum
	rest := NShards % groupNum
	ret := make([]int, groupNum)
	for i := 0; i < groupNum; i++ {
		ret[i] = avg
		if rest > 0 {
			ret[i]++
			rest--
		}
	}

	return ret
}

func (sc *ShardCtrler) checkLeader() {
	for !sc.killed() {
		sc.mu.Lock()
		if sc.isLeader() && !sc.checkedLeader {
			sc.rf.Start(Op{})
		}
		sc.checkedLeader = sc.isLeader()
		sc.mu.Unlock()

		time.Sleep(10 * time.Millisecond)
	}
}

//
// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sc *ShardCtrler) Kill() {
	atomic.StoreInt32(&sc.dead, 1)
	sc.rf.Kill()
	// Your code here, if desired.
}

func (sc *ShardCtrler) killed() bool {
	z := atomic.LoadInt32(&sc.dead)
	return z == 1
}

// needed by shardsc tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	labgob.Register(JoinArgs{})
	labgob.Register(LeaveArgs{})
	labgob.Register(MoveArgs{})
	labgob.Register(QueryArgs{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.Appliedlogs = make(map[int]interface{})
	sc.Requiredlogs = make(map[int]int)
	sc.ClientSerialNumber = make(map[(int64)]int)

	go sc.readFromApplyCh()

	go sc.checkLeader()

	return sc
}
