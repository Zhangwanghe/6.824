package raft

type Entry struct {
	Term    int
	Command interface{}
}

type Log struct {
	logs       []Entry
	startIndex int
}

func makeEmptyLog() Log {
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
	if index == 0 {
		return index - 1, -1, log.logs[index:]
	} else {
		return index - 1, log.logs[index-1].Term, log.logs[index:]
	}
}

func hasPrevLogNL(log *Log, index int, term int) bool {
	if index < 0 {
		return true
	}

	return len(log.logs) > index && log.logs[index].Term == term
}

func appendAndRemoveConflictinLogFromIndexNL(log *Log, index int, entries []Entry) {
	for i := index + 1; i < len(log.logs); i++ {
		if log.logs[i].Term != entries[i-index-1].Term {
			log.logs = append(log.logs[:i-1], entries[i-index-1:]...)
			break
		}
	}
}

func getCommitLogNL(log *Log, prevCommit int, newCommit int) []ApplyMsg {
	ret := make([]ApplyMsg, newCommit-prevCommit)
	for i := prevCommit + 1; i <= newCommit; i++ {
		ret[i].Command = log.logs[i].Command
		ret[i].CommandIndex = i
		ret[i].CommandValid = true
	}
	return ret
}
