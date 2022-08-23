package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

type Coordinator struct {
	// Your definitions here.
	files                []string
	nReduce              int
	unallocatedForMap    int
	failedForMap         []int
	unallocatedForReduce int
	failedForReduce      []int
	mu                   sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) GetTask(args *MRTaskArgs, reply *MRTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// todo deal with wait when map tasks are not all finished but all assigned
	if !c.getMapFinishedNL() {
		reply.ReduceNumber = c.nReduce
		reply.TaskType = MapTask
		if len(c.failedForMap) != 0 {
			reply.TaskNumebr = c.failedForMap[0]
			c.failedForMap = c.failedForMap[1:]
		} else {
			reply.TaskNumebr = c.unallocatedForMap
			c.unallocatedForMap += 1
		}
		reply.FileName = c.files[reply.TaskNumebr]
	} else if !c.getReduceFinishedNL() {
		reply.TaskType = ReduceTask
	} else {
		reply.TaskType = ExitTask
	}

	return nil
}

func (c *Coordinator) WriteResult(args *MRResultArgs, reply *MRResultReply) error {
	switch {
	case args.TaskType == MapTask:
		c.dealWithMapResult(args.TaskNumber, args.Result)
	case args.TaskType == ReduceTask:
		c.dealWithReduceResult(args.TaskNumber, args.Result)
	}
	return nil
}

func (c *Coordinator) dealWithMapResult(mapIndex int, result bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !result {
		c.failedForMap = append(c.failedForMap, mapIndex)
	}
}

func (c *Coordinator) dealWithReduceResult(mapIndex int, result bool) {

}

// NL denotes Not Lock
func (c *Coordinator) getMapFinishedNL() bool {
	return c.unallocatedForMap == len(c.files) && len(c.failedForMap) == 0
}

func (c *Coordinator) getReduceFinishedNL() bool {
	return c.unallocatedForReduce == c.nReduce && len(c.failedForReduce) == 0
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
		unallocatedForReduce: 0,
		failedForReduce:      make([]int, 0),
	}

	c.server()
	return &c
}
