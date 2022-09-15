package shardctrler

import (
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
}

type Op struct {
	// Your data here.
	OpType       string
	Client       int64
	SerialNumber int
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
}

func (sc *ShardCtrler) hasExecuted(client int64, serialNumber int, configNumber int) (bool, Config) {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	executed := false
	var config Config
	if sc.ClientSerialNumber[client] >= serialNumber {
		executed = true
		if configNumber == -1 || configNumber >= len(sc.configs) {
			// we don't deal with Query when none config is added
			config = sc.configs[len(sc.configs)-1]
		} else {
			config = sc.configs[configNumber]
		}
	}

	return executed, config
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
	return ok && appliedCommand == command
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

		if op.OpType == "Put" {
			// todo
		}

		sc.ClientSerialNumber[op.Client] = op.SerialNumber
	}
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
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.configs = make([]Config, 0)
	sc.Appliedlogs = make(map[int]interface{})
	sc.Requiredlogs = make(map[int]int)
	sc.ClientSerialNumber = make(map[(int64)]int)

	go sc.readFromApplyCh()

	go sc.checkLeader()

	return sc
}
