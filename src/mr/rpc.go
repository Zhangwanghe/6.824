package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"fmt"
	"os"
	"path/filepath"
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
	DeleteFiles bool
}

const IntermediateFilePrefix string = "MR"
const OutputFilePrefix string = "mr-out"

func getIntermediateFileName(mapIndex int, reduceIndex int) string {
	return fmt.Sprintf("%s-%d-%d", IntermediateFilePrefix, mapIndex, reduceIndex)
}

func getAllFilesForReduce(reduceIndex int) (matches []string, err error) {
	s := fmt.Sprintf("%s-*-%d", IntermediateFilePrefix, reduceIndex)
	return filepath.Glob(s)
}

func getOutputFileName(reduceIndex int) string {
	return fmt.Sprintf("%s-%d", OutputFilePrefix, reduceIndex)
}

func clearFilesForMap(mapIndex int) {
	s := fmt.Sprintf("%s-%d-*", IntermediateFilePrefix, mapIndex)
	files, err := filepath.Glob(s)
	if err != nil {
		return
	}

	for _, filename := range files {
		os.Remove(filename)
	}
}

func clearFilesForReduce(reduceIndex int) {
	files, err := getAllFilesForReduce(reduceIndex)
	if err != nil {
		return
	}

	for _, filename := range files {
		os.Remove(filename)
	}
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
