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

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// NL denotes Not Lock
func (c *Coordinator) getMapFinishedNL() bool {
	return c.unallocatedForMap == len(c.files) && len(c.failedForMap) == 0
}

func (c *Coordinator) getReduceFinishedNL() bool {
	return c.unallocatedForReduce == c.nReduce && len(c.failedForReduce) == 0
}

func (c *Coordinator) GetTask(args *MRArgs, reply *MRReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.getMapFinishedNL() {
		reply.TaskType = MapTask
	} else if !c.getReduceFinishedNL() {
		reply.TaskType = ReduceTask
	} else {
		reply.TaskType = ExitTask
	}

	return nil
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
	c := Coordinator{
		files:                files,
		nReduce:              nReduce,
		unallocatedForMap:    0,
		failedForMap:         make([]int, 0),
		unallocatedForReduce: 0,
		failedForReduce:      make([]int, 0),
	}

	// Your code here.

	c.server()
	return &c
}
