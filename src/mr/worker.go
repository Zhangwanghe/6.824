package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	for {
		args := MRTaskArgs{}
		reply := MRTaskReply{}
		ok := call("Coordinator.GetTask", &args, &reply)
		ret := false
		if ok {
			switch {
			case reply.TaskType == MapTask:
				ret = dealWithMapTask(mapf, reply.ReduceNumber, reply.FileName, reply.TaskNumebr)
			case reply.TaskType == ReduceTask:
				dealWithReduceTask(reducef, &reply)
			case reply.TaskType == ExitTask:
				return
			}
		} else {
			return
		}

		if !writeBack(reply.TaskType, reply.TaskNumebr, ret) {
			return
		}
	}
}

func dealWithMapTask(mapf func(string, string) []KeyValue,
	nReduce int,
	filename string,
	mapIndex int) bool {
	file, err := os.Open(filename)
	if err != nil {
		log.Print("cannot open %v", filename)
		return false
	}

	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Print("cannot read %v", filename)
		return false
	}
	file.Close()

	kva := mapf(filename, string(content))
	reduceIndexToKv := make(map[int][]KeyValue)
	for _, kv := range kva {
		index := ihash(kv.Key) % nReduce
		reduceIndexToKv[index] = append(reduceIndexToKv[index], kv)
	}

	for reduceIndex, kvs := range reduceIndexToKv {
		file, err := ioutil.TempFile("./", "map-temp-*")
		if err != nil {
			log.Print("cannot create temp file")
			return false
		}

		enc := json.NewEncoder(file)
		for _, kv := range kvs {
			err := enc.Encode(&kv)
			if err != nil {
				log.Print("encode err")
			}
		}

		file.Close()

		os.Rename(file.Name(), getIntermediateFileName(mapIndex, reduceIndex))
	}

	return true
}

func dealWithReduceTask(reducef func(string, []string) string, reply *MRTaskReply) {

}

func writeBack(taskType int, taskNumber int, result bool) bool {
	args := MRResultArgs{taskType, taskNumber, result}
	reply := MRTaskReply{}
	ok := call("Coordinator.WriteResult", &args, &reply)
	return ok
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
