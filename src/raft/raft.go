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

const HeartBeatDuration = time.Duration(150)
const CheckHeartBeatDuration = time.Duration(10)

type Raft struct {
	mu                       sync.Mutex          // Lock to protect shared access to this peer's state
	peers                    []*labrpc.ClientEnd // RPC end points of all peers
	persister                *Persister          // Object to hold this peer's persisted state
	me                       int                 // this peer's index into peers[]
	dead                     int32               // set by Kill()
	currentTerm              int
	role                     Role
	votedFor                 int
	lastElectionTerm         int
	lastRecieveHeartBeatTime time.Time
	lastSendHeartBeatTime    time.Time

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
	Term        int
	CandidateId int
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
	} else if rf.votedFor == -1 {
		// todo check whether log is up-to-date
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
	}
}

type AppendEntriesArgs struct {
	// Your data here (2A, 2B).
	Term     int
	LeaderId int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type AppendEntriesReply struct {
	// Your data here (2A).
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (2A, 2B).z
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm

	if args.Term < reply.Term {
		reply.Success = false
		return
	}

	// todo log conflicting and not containing

	rf.lastRecieveHeartBeatTime = time.Now()

	// todo append new entries

	// todo update commitIndex
}

func (rf *Raft) sendAppendEntries(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
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

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	rf.mu.Lock()
	rf.currentTerm++
	term := rf.currentTerm
	rf.votedFor = rf.me
	rf.lastElectionTerm = term
	rf.mu.Unlock()

	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().

		go rf.election(term)

		rf.mu.Lock()
		valid := rf.isValidElectionNL(term)
		rf.mu.Unlock()

		if !valid {
			break
		}

		electionTimeOut := (200 + rand.Intn(200))
		time.Sleep(time.Duration(electionTimeOut) * time.Millisecond)
	}
}

func (rf *Raft) election(term int) {
	ch := make(chan RequestVoteReply, len(rf.peers)-1)

	for index, _ := range rf.peers {
		if index == rf.me {
			continue
		}

		go func(ch chan RequestVoteReply, term int, index int) {
			args := RequestVoteArgs{term, rf.me}
			reply := RequestVoteReply{term, false}
			rf.sendRequestVote(index, &args, &reply)
			ch <- reply
		}(ch, term, index)
	}

	succeed := 1
	for reply := range ch {
		ret := rf.dealWithRequestVoteReply(term, &succeed, &reply)
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
func (rf *Raft) dealWithRequestVoteReply(term int, succeed *int, reply *RequestVoteReply) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if !rf.isValidElectionNL(term) {
		return false
	}

	// I'm still a candidate and the next election has not yet started
	if reply.VoteGranted {
		*succeed++
		if *succeed > len(rf.peers)/2 {
			rf.role = Leader
			return false
		}
	} else if reply.Term > term {
		rf.currentTerm = term
		rf.convertToFollowerNL()

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
		if rf.lastRecieveHeartBeatTime.Add(HeartBeatDuration).Before(time.Now()) && rf.role == Follower {
			rf.convertToCandidateNL()
		}
		rf.mu.Unlock()

		time.Sleep(CheckHeartBeatDuration * time.Millisecond)
	}
}

func (rf *Raft) convertToCandidateNL() {
	rf.role = Candidate
	// todo check this condition
	if rf.lastElectionTerm != rf.currentTerm {
		go rf.ticker()
	}
}

func (rf *Raft) convertToFollowerNL() {
	rf.role = Follower
	rf.votedFor = -1
}

func (rf *Raft) convertToLeaderNL() {
	rf.role = Leader

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
	rf.lastElectionTerm = -1
	rf.lastRecieveHeartBeatTime = time.Now()
	rf.lastSendHeartBeatTime = time.Now()

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	rf.convertToCandidateNL()

	go rf.heartBeatCheck()

	return rf
}
