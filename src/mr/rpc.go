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
	ExitTask
)

type MRArgs struct {
}

type MRReply struct {
	TaskType     int
	ReduceNumber int
	TaskNumebr   int
	FileName     string
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
