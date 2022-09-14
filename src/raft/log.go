package raft

import "fmt"

type Entry struct {
	Term    int
	Command interface{}
}

func (e *Entry) String() string {
	return fmt.Sprintf("term = %d, command = %v", e.Term, e.Command)
}

type Log struct {
	// save the last log term from snapshot in log[0]
	Logs       []Entry
	StartIndex int
}

func makeEmptyLog() Log {
	// log start from 1
	return Log{make([]Entry, 1), 0}
}

func getTotalIndex(log *Log, index int) int {
	return log.StartIndex + index
}

func getIndexInCurLog(log *Log, index int) int {
	return index - log.StartIndex
}

func appendLogNL(log *Log, term int, command interface{}) {
	log.Logs = append(log.Logs, Entry{term, command})
}

func getLastLogIndexNL(log *Log) int {
	return getTotalIndex(log, len(log.Logs)-1)
}

func getLastLogTermNL(log *Log) int {
	return log.Logs[len(log.Logs)-1].Term
}

func getPrevLogAndNewEntriesNL(log *Log, index int) (int, int, []Entry) {
	index = getIndexInCurLog(log, index)
	if index <= 0 {
		index = 1
	}

	entries := make([]Entry, len(log.Logs)-index)
	copy(entries, log.Logs[index:])
	return getTotalIndex(log, index-1), log.Logs[index-1].Term, entries
}

func hasPrevLogNL(log *Log, index int, term int) bool {
	index = getIndexInCurLog(log, index)
	if index < 0 {
		return false
	}

	return len(log.Logs) > index && log.Logs[index].Term == term
}

func getLogInfoBeforeConflictingNL(log *Log, index int) (int, int) {
	index = getIndexInCurLog(log, index)
	if index < 0 {
		return log.Logs[0].Term, log.StartIndex
	}

	if len(log.Logs) <= index {
		index = len(log.Logs) - 1
	}

	conflictingTerm := log.Logs[index].Term
	conflictingIndex := 0

	for i := index; i >= 0; i-- {
		if conflictingTerm != log.Logs[i].Term {
			conflictingIndex = i
			break
		}
	}

	return conflictingTerm, getTotalIndex(log, conflictingIndex)
}

func appendAndRemoveConflictinLogFromIndexNL(log *Log, lastLogIndex int, entries []Entry) {
	if len(entries) == 0 {
		// heartbeat
		return
	}

	lastLogIndex = getIndexInCurLog(log, lastLogIndex)
	i := lastLogIndex + 1
	for ; i < Min(len(log.Logs), lastLogIndex+1+len(entries)); i++ {
		if log.Logs[i].Term != entries[i-lastLogIndex-1].Term {
			break
		}
	}

	if i-lastLogIndex-1 >= len(entries) {
		return
	}

	log.Logs = append(log.Logs[:i], entries[i-lastLogIndex-1:]...)
}

func getCommitLogNL(log *Log, prevCommit int, newCommit int) []ApplyMsg {
	prevCommit = Max(getIndexInCurLog(log, prevCommit), 0)
	newCommit = Min(getIndexInCurLog(log, newCommit), len(log.Logs)-1)

	if prevCommit >= newCommit {
		return make([]ApplyMsg, 0)
	}

	ret := make([]ApplyMsg, newCommit-prevCommit)
	for i := prevCommit + 1; i <= newCommit; i++ {
		ret[i-prevCommit-1].Command = log.Logs[i].Command
		ret[i-prevCommit-1].CommandIndex = getTotalIndex(log, i)
		ret[i-prevCommit-1].CommandValid = true
	}
	return ret
}

func getLastLogIndexForTermNL(log *Log, term int) int {
	var left = 0
	var right = len(log.Logs) - 1

	for left+1 < right {
		var mid = (left + right) / 2
		if log.Logs[mid].Term > term {
			right = mid
		} else {
			left = mid
		}
	}

	return getTotalIndex(log, left)
}

func getTermForGivenIndexNL(log *Log, index int) int {
	index = getIndexInCurLog(log, index)
	return log.Logs[index].Term
}

func makeSnapshotNL(log *Log, index int) {
	log.Logs[0].Term = log.Logs[getIndexInCurLog(log, index)].Term
	log.Logs = append(log.Logs[0:1], log.Logs[getIndexInCurLog(log, index)+1:]...)
	log.StartIndex = index
}

func getStartIndexNL(log *Log) int {
	return log.StartIndex
}

func getStartTermNL(log *Log) int {
	return log.Logs[0].Term
}
