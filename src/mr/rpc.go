package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"fmt"
	"os"
	"strconv"
)

// Add your RPC definitions here.
// TaskType
const (
	MapTask = iota
	ReduceTask
	WaitTask
	ExitTask
)

type MRTaskArgs struct {
}

type MRTaskReply struct {
	TaskType     int
	ReduceNumber int
	TaskNumber   int
	FileName     string
}

type MRResultArgs struct {
	TaskType   int
	TaskNumber int
	Result     bool
}

type MRResultReply struct {
}

const IntermediateFilePrefix string = "MR"

func getIntermediateFileName(mapIndex int, reduceIndex int) string {
	return fmt.Sprintf("%s-%d-%d", IntermediateFilePrefix, mapIndex, reduceIndex)
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
