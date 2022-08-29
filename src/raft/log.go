package raft

import "fmt"

type Entry struct {
	Term    int
	Command interface{}
}

func (e *Entry) String() string {
	return fmt.Sprintf("T %v", e.Term)
}

type Log struct {
	logs       []Entry
	startIndex int
}

func makeEmptyLog() Log {
	// log start from 1
	return Log{make([]Entry, 1), 0}
}

func appendLogNL(log *Log, term int, command interface{}) {
	log.logs = append(log.logs, Entry{term, command})
}

func getLastLogIndexNL(log *Log) int {
	return len(log.logs) - 1
}

func getLastLogTermNL(log *Log) int {
	return log.logs[len(log.logs)-1].Term
}

func getPrevLogAndNewEntriesNL(log *Log, index int) (int, int, []Entry) {
	if index <= 1 {
		return index - 1, -1, log.logs[index:]
	} else {
		return index - 1, log.logs[index-1].Term, log.logs[index:]
	}
}

func hasPrevLogNL(log *Log, index int, term int) bool {
	if index <= 0 {
		return true
	}

	return len(log.logs) > index && log.logs[index].Term == term
}

func appendAndRemoveConflictinLogFromIndexNL(log *Log, lastLogIndex int, entries []Entry) {
	if len(entries) == 0 {
		// heartbeat
		return
	}

	for i := lastLogIndex + 1; i < len(log.logs); i++ {
		if i-lastLogIndex-1 < 0 {
			log.logs = append(log.logs[:i-1], entries...)
			return
		} else if log.logs[i].Term != entries[i-lastLogIndex-1].Term {
			log.logs = append(log.logs[:i-1], entries[i-lastLogIndex-1:]...)
			return
		}
	}

	log.logs = append(log.logs, entries...)
}

func getCommitLogNL(log *Log, prevCommit int, newCommit int) []ApplyMsg {
	ret := make([]ApplyMsg, newCommit-prevCommit)
	for i := prevCommit + 1; i <= newCommit; i++ {
		ret[i-prevCommit-1].Command = log.logs[i].Command
		ret[i-prevCommit-1].CommandIndex = i
		ret[i-prevCommit-1].CommandValid = true
	}
	return ret
}
