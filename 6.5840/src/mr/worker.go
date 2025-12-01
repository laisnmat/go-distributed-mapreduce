package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type WorkerStruct struct {
	WorkerAddress      string
	CoordinatorAddress string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(workerAddress string, coordinatorAddress string, mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	w := &WorkerStruct{
		WorkerAddress:      workerAddress,
		CoordinatorAddress: coordinatorAddress,
	}
	w.server()
	for {
		args := TaskArgs{}
		reply := TaskReply{}
		ret := call("Coordinator.GetTask", coordinatorAddress, &args, &reply)
		if !ret {
			log.Println("Coordinator unavailable, exiting worker")
			return
		}
		switch reply.TaskType {
		case MapTask:
			handleMapTask(w, reply, mapf)
			notifyCoordinator(w, MapTask, reply)
		case ReduceTask:
			handleReduceTask(reply, reducef)
			notifyCoordinator(w, ReduceTask, reply)
		case NoAvailableTask:
			time.Sleep(time.Second)
		case Exit:
			return
		}
	}
}

func handleMapTask(w *WorkerStruct, reply TaskReply, mapf func(string, string) []KeyValue) {
	filename := reply.File
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	kva := mapf(filename, string(content))
	buckets := make([][]KeyValue, reply.NReduce)
	for _, kv := range kva {
		h := ihash(kv.Key) % reply.NReduce
		buckets[h] = append(buckets[h], kv)
	}
	writeIntermediateFiles(buckets, reply)
}

func notifyCoordinator(w *WorkerStruct, taskType TaskType, reply TaskReply) {
	doneArgs := TaskDoneArgs{
		TaskId:        reply.TaskId,
		TaskType:      taskType,
		WorkerAddress: w.WorkerAddress,
	}
	call("Coordinator.SetTaskDone", w.CoordinatorAddress, &doneArgs, &TaskDoneReply{})
}

func readIntermediateFiles(reply TaskReply) []KeyValue {
	intermediate := []KeyValue{}
	for mapId, addr := range reply.MapsAddr {
		filename := fmt.Sprintf("mr-%d-%d", mapId, reply.TaskId)
		getArgs := GetIntermediateArgs{
			Filename: filename,
		}
		getReply := GetIntermediateReply{}
		ret := call("Worker.GetIntermediate", addr, &getArgs, &getReply)
		if ret {
			intermediate = append(intermediate, getReply.Intermediate...)
		} else {
			log.Printf("Error: cannot call worker %s", addr)
		}
	}
	return intermediate
}

func (w *WorkerStruct) GetIntermediate(args *GetIntermediateArgs, reply *GetIntermediateReply) error {
	intermediate := []KeyValue{}
	file, err := os.Open(args.Filename)
	if err != nil {
		log.Printf("Cannot open intermediate file %s: %v", args.Filename, err)
		reply.Intermediate = intermediate
		return nil
	}
	defer file.Close()
	dec := json.NewDecoder(file)
	for {
		var kv KeyValue
		if err := dec.Decode(&kv); err != nil {
			break
		}
		intermediate = append(intermediate, kv)
	}
	reply.Intermediate = intermediate
	return nil
}

func writeIntermediateFiles(buckets [][]KeyValue, reply TaskReply) {
	for i := 0; i < reply.NReduce; i++ {
		oname := fmt.Sprintf("mr-%d-%d", reply.TaskId, i)
		ofile, tempErr := ioutil.TempFile("", "mr-tmp-*")
		if tempErr != nil {
			log.Fatal("TempFile error:", tempErr)
		}
		enc := json.NewEncoder(ofile)
		for _, kv := range buckets[i] {
			encodeErr := enc.Encode(&kv)
			if encodeErr != nil {
				log.Fatal("Encode error:", encodeErr)
			}
		}
		ofile.Close()
		moveFileErr := MoveFile(ofile.Name(), oname)
		if moveFileErr != nil {
			log.Fatal("MoveFile error:", moveFileErr)
		}
	}
}

func handleReduceTask(reply TaskReply, reducef func(string, []string) string) {
	intermediate := readIntermediateFiles(reply)
	sort.Sort(ByKey(intermediate))
	oname := fmt.Sprintf("mr-out-%d", reply.TaskId)
	ofile, tempErr := ioutil.TempFile("", "mr-tmp-out-*")
	if tempErr != nil {
		log.Fatal("TempFile error:", tempErr)
	}
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}
	ofile.Close()
	moveFileErr := MoveFile(ofile.Name(), oname)
	if moveFileErr != nil {
		log.Fatal("MoveFile error:", moveFileErr)
	}
}

func (w *WorkerStruct) server() {
	rpc.Register(w)
	rpc.HandleHTTP()
	l, err := net.Listen("tcp", w.WorkerAddress)
	if err != nil {
		log.Fatalf("Worker RPC listen error: %v", err)
	}
	go http.Serve(l, nil)
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(address string, rpcname string, args interface{}, reply interface{}) bool {
	c, err := rpc.DialHTTP("tcp", address)
	//sockname := coordinatorSock()
	//c, err := rpc.DialHTTP("unix", sockname)
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
