package raft

type Entry struct {
	term    int
	command interface{}
}

type Log struct {
	logs       []Entry
	startIndex int
}

func makeEmptyLog() Log {
	return Log{make([]Entry, 1), 0}
}

func getLastLogIndexNL(log *Log) int {
	return len(log.logs) - 1
}

func getLastLogTermNL(log *Log) int {
	return log.logs[len(log.logs)-1].term
}

func getPrevLogAndNewEntriesNL(log *Log, index int) (int, int, []Entry) {
	if index == 0 {
		return index - 1, -1, log.logs[index:]
	} else {
		return index - 1, log.logs[index-1].term, log.logs[index:]
	}
}

func hasPrevLogNL(log *Log, index int, term int) bool {
	if index < 0 {
		return true
	}

	return len(log.logs) > index && log.logs[index].term == term
}

func appendAndRemoveConflictinLogFromIndexNL(log *Log, index int, entries []Entry) {
	for i := index + 1; i < len(log.logs); i++ {
		if log.logs[i].term != entries[i-index-1].term {
			log.logs = append(log.logs[:i-1], entries[i-index-1:]...)
			break
		}
	}
}
