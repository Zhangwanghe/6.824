package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	files             []string
	nReduce           int
	unallocatedForMap int
	// todo better to use orderedmap since we have to deal with all by time and erase any
	failedForMap         []int
	excutingForMap       map[int]int
	successForMap        map[int]int
	unallocatedForReduce int
	failedForReduce      []int
	excutingForReduce    map[int]int
	successForReduce     map[int]int
	mu                   sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) GetTask(args *MRTaskArgs, reply *MRTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.getMapFinishedNL() {
		if c.canAssignMapTaskNL() {
			c.assignMapTaskNL(reply)
		} else {
			c.assignWaitTaskNL(reply)
		}
	} else if !c.getReduceFinishedNL() {
		if c.canAssignReduceTaskNL() {
			c.assignReduceTaskNL(reply)
		} else {
			c.assignWaitTaskNL(reply)
		}
	} else {
		c.assignExitTaskNL(reply)
	}

	return nil
}

func (c *Coordinator) assignMapTaskNL(reply *MRTaskReply) {
	reply.ReduceNumber = c.nReduce
	reply.TaskType = MapTask

	if len(c.failedForMap) != 0 {
		reply.TaskNumber = c.failedForMap[0]
		c.failedForMap = c.failedForMap[1:]
	} else {
		reply.TaskNumber = c.unallocatedForMap
		c.unallocatedForMap += 1
	}

	reply.FileName = c.files[reply.TaskNumber]
	c.excutingForMap[reply.TaskNumber] = 1

	go c.dealWithMapTaskTimeout(reply.ReduceNumber)
}

func (c *Coordinator) dealWithMapTaskTimeout(index int) {
	time.Sleep(10 * time.Second)

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.isMapTaskSuccessNL(index) {
		c.dealWithMapResultNL(index, false)
	}
}

func (c *Coordinator) assignWaitTaskNL(reply *MRTaskReply) {
	reply.TaskType = WaitTask
}

func (c *Coordinator) assignReduceTaskNL(reply *MRTaskReply) {
	reply.TaskType = ReduceTask

	if len(c.failedForReduce) != 0 {
		reply.TaskNumber = c.failedForReduce[0]
		c.failedForReduce = c.failedForReduce[1:]
	} else {
		reply.TaskNumber = c.unallocatedForReduce
		c.unallocatedForReduce += 1
	}

	c.excutingForReduce[reply.TaskNumber] = 1

	go c.dealWithReduceTaskTimeout(reply.ReduceNumber)
}

func (c *Coordinator) dealWithReduceTaskTimeout(index int) {
	time.Sleep(10 * time.Second)

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.isReduceTaskSuccessNL(index) {
		c.dealWithReduceResultNL(index, false)
	}
}

func (c *Coordinator) assignExitTaskNL(reply *MRTaskReply) {
	reply.TaskType = ExitTask
}

func (c *Coordinator) WriteResult(args *MRResultArgs, reply *MRResultReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	switch {
	case args.TaskType == MapTask:
		c.dealWithMapResultNL(args.TaskNumber, args.Result)
	case args.TaskType == ReduceTask:
		c.dealWithReduceResultNL(args.TaskNumber, args.Result)
	}
	return nil
}

func (c *Coordinator) dealWithMapResultNL(mapIndex int, result bool) {
	if _, ok := c.successForMap[mapIndex]; ok {
		return
	}

	if !result {
		c.failedForMap = append(c.failedForMap, mapIndex)
	} else {
		c.successForMap[mapIndex] = 1
	}

	delete(c.excutingForMap, mapIndex)
}

func (c *Coordinator) dealWithReduceResultNL(reduceIndex int, result bool) {
	if _, ok := c.successForReduce[reduceIndex]; ok {
		return
	}

	if !result {
		c.failedForReduce = append(c.failedForReduce, reduceIndex)
	} else {
		c.successForReduce[reduceIndex] = 1
	}

	delete(c.excutingForReduce, reduceIndex)
}

// NL denotes Not Lock
func (c *Coordinator) getMapFinishedNL() bool {
	return len(c.successForMap) == len(c.files)
}

func (c *Coordinator) canAssignMapTaskNL() bool {
	return c.unallocatedForMap != len(c.files) || len(c.failedForMap) != 0
}

func (c *Coordinator) isMapTaskSuccessNL(mapIndex int) bool {
	if _, ok := c.successForMap[mapIndex]; ok {
		return true
	}

	return false
}

func (c *Coordinator) getReduceFinishedNL() bool {
	return len(c.successForReduce) == c.nReduce
}

func (c *Coordinator) canAssignReduceTaskNL() bool {
	return c.unallocatedForReduce != c.nReduce || len(c.failedForReduce) != 0
}

func (c *Coordinator) isReduceTaskSuccessNL(reduceIndex int) bool {
	if _, ok := c.successForMap[reduceIndex]; ok {
		return true
	}

	return false
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.getReduceFinishedNL() {
		ret = true
	}

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	// Your code here.
	c := Coordinator{
		files:                files,
		nReduce:              nReduce,
		unallocatedForMap:    0,
		failedForMap:         make([]int, 0),
		excutingForMap:       make(map[int]int),
		successForMap:        make(map[int]int),
		unallocatedForReduce: 0,
		failedForReduce:      make([]int, 0),
		excutingForReduce:    make(map[int]int),
		successForReduce:     make(map[int]int),
	}

	c.server()
	return &c
}
