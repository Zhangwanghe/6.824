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
	entries := make([]Entry, len(log.logs)-index)
	copy(entries, log.logs[index:])

	if index <= 1 {
		return index - 1, -1, entries
	} else {
		return index - 1, log.logs[index-1].Term, entries
	}
}

func hasPrevLogNL(log *Log, index int, term int) bool {
	if index <= 0 {
		return true
	}

	return len(log.logs) > index && log.logs[index].Term == term
}

func getLogInfoBeforeConflicting(log *Log) (int, int) {
	conflictingTerm := log.logs[len(log.logs)-1].Term
	conflictingIndex := len(log.logs) - 1

	for i := len(log.logs) - 2; i >= 1; i-- {
		if conflictingTerm == log.logs[i].Term {
			conflictingIndex = i
		} else {
			break
		}
	}

	return conflictingTerm, conflictingIndex
}

func appendAndRemoveConflictinLogFromIndexNL(log *Log, lastLogIndex int, entries []Entry) {
	if len(entries) == 0 {
		// heartbeat
		return
	}

	log.logs = append(log.logs[:lastLogIndex+1], entries...)
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

func getLastLogIndexForTerm(log *Log, term int) int {
	var left = 0
	var right = len(log.logs) - 1

	for left+1 < right {
		var mid = (left + right) / 2
		if log.logs[mid].Term > term {
			right = mid
		} else {
			left = mid
		}
	}

	if log.logs[left].Term == term {
		return left
	} else {
		return -1
	}
}
