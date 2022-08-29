package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"

	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

//
// A Go object implementing a single Raft peer.
//
type Role int

const (
	Follower  Role = 1
	Candidate Role = 2
	Leader    Role = 3
)

const HeartBeatDuration = time.Duration(150) * time.Millisecond
const CheckHeartBeatDuration = time.Duration(10) * time.Millisecond

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// non-volatile state on all
	currentTerm int
	votedFor    int
	log         Log

	// volatile state on all
	commitIndex int
	lastApplied int

	// volatile on Leader
	nextIndex  []int
	matchIndex []int

	role                     Role
	lastElectionTerm         int
	lastRecieveHeartBeatTime time.Time
	lastSendHeartBeatTime    time.Time
	commitChan               chan ApplyMsg

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.currentTerm
	isleader = rf.role == Leader

	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogTerm  int
	LastLogIndex int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).z
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm

	if args.Term < reply.Term {
		reply.VoteGranted = false
		return
	} else if args.Term > reply.Term {
		rf.convertToFollowerNL(args.Term)
	}

	// todo whether to add an empty log after becoming leader
	if rf.votedFor == -1 && !rf.isLogUpToDateNL(args.LastLogIndex, args.LastLogTerm) {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
	}
}

func (rf *Raft) isLogUpToDateNL(logIndex int, logTerm int) bool {
	lastLogForMe := getLastLogTermNL(&rf.log)
	return lastLogForMe > logTerm || (lastLogForMe == logTerm && getLastLogIndexNL(&rf.log) > logIndex)
}

type AppendEntriesArgs struct {
	// Your data here (2A, 2B).
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Entry
	LeaderCommit int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type AppendEntriesReply struct {
	// Your data here (2A).
	Term    int
	Success bool

	// add for synchonization
	LastLogIndex  int
	FollowerIndex int

	// add for quick recover log
	ConflictTerm                 int
	FirstLogIndexForConflictTerm int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (2A, 2B).z
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.FollowerIndex = rf.me

	if args.Term < reply.Term {
		reply.Success = false
		return
	} else {
		rf.lastRecieveHeartBeatTime = time.Now()
		rf.convertToFollowerNL(args.Term)

		if !hasPrevLogNL(&rf.log, args.PrevLogIndex, args.PrevLogTerm) {
			reply.Success = false
			reply.ConflictTerm, reply.FirstLogIndexForConflictTerm = getLogInfoBeforeConflicting(&rf.log)
			return
		}

		appendAndRemoveConflictinLogFromIndexNL(&rf.log, args.PrevLogIndex, args.Entries)
		reply.Success = true
		reply.LastLogIndex = args.PrevLogIndex + len(args.Entries)

		if args.LeaderCommit > rf.commitIndex {
			newCommitIndex := Min(args.LeaderCommit, getLastLogIndexNL(&rf.log))
			rf.commitNL(newCommitIndex)
		}

	}
}

func Min(x int, y int) int {
	if x > y {
		return y
	} else {
		return x
	}
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.currentTerm
	isLeader = rf.role == Leader

	if isLeader {
		appendLogNL(&rf.log, term, command)
		index = getLastLogIndexNL(&rf.log)
		rf.nextIndex[rf.me] = index + 1
		rf.matchIndex[rf.me] = index
		// todo should we synchronize immediately or wait for heartbeat
		go rf.synchronize(term)
	}

	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) convertToCandidateNL() {
	if rf.role == Candidate {
		return
	}

	rf.role = Candidate
	if rf.lastElectionTerm != rf.currentTerm {
		go rf.ticker()
	}
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		rf.mu.Lock()
		valid := rf.isValidElectionNL(rf.currentTerm)
		rf.mu.Unlock()

		if !valid {
			break
		}

		rf.mu.Lock()
		rf.currentTerm++
		term := rf.currentTerm
		lastLogTerm := getLastLogTermNL(&rf.log)
		lastLogIndex := getLastLogIndexNL(&rf.log)
		rf.votedFor = rf.me
		rf.lastElectionTerm = term
		rf.mu.Unlock()

		go rf.election(term, lastLogTerm, lastLogIndex)

		electionTimeOut := (200 + rand.Intn(200))
		time.Sleep(time.Duration(electionTimeOut) * time.Millisecond)
	}
}

func (rf *Raft) election(term int, lastLogTerm int, lastLogIndex int) {
	ch := make(chan RequestVoteReply)

	for index, _ := range rf.peers {
		if index == rf.me {
			continue
		}

		go func(index int) {
			args := RequestVoteArgs{term, rf.me, lastLogTerm, lastLogIndex}
			reply := RequestVoteReply{}
			rf.sendRequestVote(index, &args, &reply)
			ch <- reply
		}(index)
	}

	succeed := 1
	for i := 0; i < len(rf.peers)-1; i++ {
		reply := <-ch
		ret := rf.dealWithRequestVoteReply(term, &succeed, i+1, &reply)
		if !ret {
			break
		}
	}

}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// return value denotes whether election is still valid
func (rf *Raft) dealWithRequestVoteReply(term int, succeed *int, total int, reply *RequestVoteReply) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if !rf.isValidElectionNL(term) {
		return false
	}

	// I'm still a candidate and the next election has not yet started
	if reply.VoteGranted {
		*succeed++
		if *succeed > len(rf.peers)/2 {
			rf.convertToLeaderNL()
			return false
		}
	} else if reply.Term > rf.currentTerm {
		rf.currentTerm = term
		rf.convertToFollowerNL(reply.Term)

		return false
	} else if total-*succeed > len(rf.peers)/2 {
		// if more than half fail, stop election
		return false
	}

	return true
}

func (rf *Raft) isValidElectionNL(term int) bool {
	if rf.currentTerm != term || rf.role != Candidate {
		return false
	}

	return true
}

func (rf *Raft) heartBeatCheck() {
	for rf.killed() == false {
		rf.mu.Lock()
		if rf.isHeartBeatTimeOutNL() {
			rf.convertToCandidateNL()
		}
		rf.mu.Unlock()

		time.Sleep(CheckHeartBeatDuration)
	}
}

func (rf *Raft) isHeartBeatTimeOutNL() bool {
	return rf.lastRecieveHeartBeatTime.Add(HeartBeatDuration*2).Before(time.Now()) && rf.role == Follower
}

func (rf *Raft) convertToLeaderNL() {
	if rf.role == Leader {
		return
	}

	rf.role = Leader
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = getLastLogIndexNL(&rf.log) + 1
		rf.matchIndex[i] = 0
	}
	go rf.heartBeat()
}

func (rf *Raft) heartBeat() {
	for {
		rf.mu.Lock()
		isLeader := rf.role == Leader
		term := rf.currentTerm
		rf.mu.Unlock()

		go rf.synchronize(term)

		if !isLeader {
			break
		}

		for {
			time.Sleep(CheckHeartBeatDuration)

			rf.mu.Lock()
			shouldHearBeat := rf.shouldHeartBeatNL()
			rf.mu.Unlock()

			if shouldHearBeat {
				break
			}
		}
	}
}

func (rf *Raft) shouldHeartBeatNL() bool {
	return rf.lastSendHeartBeatTime.Add(HeartBeatDuration).Before(time.Now())
}

func (rf *Raft) synchronize(term int) {
	rf.mu.Lock()
	rf.lastSendHeartBeatTime = time.Now()
	rf.mu.Unlock()

	ch := make(chan AppendEntriesReply)
	for index, _ := range rf.peers {
		if index == rf.me {
			continue
		}

		rf.mu.Lock()
		isLeader := rf.role == Leader
		rf.mu.Unlock()

		if !isLeader {
			break
		}

		go func(ch chan AppendEntriesReply, term int, index int) {
			args := rf.makeAppendEntriesArgs(term, index)
			reply := AppendEntriesReply{}
			ok := rf.sendAppendEntries(index, &args, &reply)
			// set followerIndex to -1
			if !ok {
				reply.FollowerIndex = -1
			}
			ch <- reply
		}(ch, term, index)
	}

	for i := 0; i < len(rf.peers)-1; i++ {
		reply := <-ch
		ret := rf.dealWithAppendEntriesReply(term, &reply)
		if !ret {
			break
		}
	}
}

func (rf *Raft) makeAppendEntriesArgs(term int, index int) AppendEntriesArgs {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	ret := AppendEntriesArgs{}
	ret.Term = term
	ret.LeaderId = rf.me
	ret.PrevLogIndex, ret.PrevLogTerm, ret.Entries = getPrevLogAndNewEntriesNL(&rf.log, rf.nextIndex[index])
	ret.LeaderCommit = rf.commitIndex

	return ret
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) dealWithAppendEntriesReply(term int, reply *AppendEntriesReply) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if !rf.isValidAppendNL(term) {
		return false
	}

	if reply.Term > rf.currentTerm {
		rf.currentTerm = term
		rf.convertToFollowerNL(reply.Term)

		return false
	}

	// update index
	if reply.Success {
		rf.matchIndex[reply.FollowerIndex] = reply.LastLogIndex
		rf.nextIndex[reply.FollowerIndex] = reply.LastLogIndex + 1
		rf.leaderCommitNL(reply.FollowerIndex)
	} else if reply.FollowerIndex != -1 {
		// since term inconsistency has been disposed above
		// we can assume if we are here, there is log inconsistency
		// todo optimized by using a map to save term-last log index relation
		logIndex := getLastLogIndexForTerm(&rf.log, term)
		if logIndex != -1 {
			rf.nextIndex[reply.FollowerIndex] = logIndex + 1
		} else {
			rf.nextIndex[reply.FollowerIndex] = reply.FirstLogIndexForConflictTerm
		}
	}

	return true
}

func (rf *Raft) isValidAppendNL(term int) bool {
	if rf.currentTerm != term || rf.role != Leader {
		return false
	}

	return true
}

func (rf *Raft) leaderCommitNL(followerIndex int) {
	if rf.matchIndex[followerIndex] <= rf.commitIndex {
		return
	}

	sortedIndex := make([]int, len(rf.peers))
	copy(sortedIndex, rf.matchIndex)
	sort.Ints(sortedIndex)

	if sortedIndex[len(sortedIndex)/2] > rf.commitIndex {
		rf.commitNL(sortedIndex[len(sortedIndex)/2])
	}
}

func (rf *Raft) commitNL(newCommitIndex int) {
	msgs := getCommitLogNL(&rf.log, rf.commitIndex, newCommitIndex)
	for _, msg := range msgs {
		rf.commitChan <- msg
	}
	rf.commitIndex = newCommitIndex
}

func (rf *Raft) convertToFollowerNL(term int) {
	if rf.role == Follower {
		return
	}

	rf.role = Follower
	rf.votedFor = -1
	rf.currentTerm = term
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	Init()
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = makeEmptyLog()
	rf.lastApplied = 0
	rf.commitIndex = 0
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.lastElectionTerm = -1
	rf.lastRecieveHeartBeatTime = time.Now()
	rf.lastSendHeartBeatTime = time.Now()
	rf.commitChan = applyCh

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	rf.convertToCandidateNL()

	go rf.heartBeatCheck()

	return rf
}
