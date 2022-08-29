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

func getLastLogIndex(log *Log) int {
	return len(log.logs) - 1
}

func getPrevLog(log *Log, index int) (int, int, []Entry) {
	if index == 0 {
		return index - 1, -1, log.logs[index:]
	} else {
		return index - 1, log.logs[index-1].term, log.logs[index:]
	}
}
