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
	Listener           net.Listener
}

const retries = 2

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
	defer closeListener(w)
	for {
		args := TaskArgs{WorkerAddress: workerAddress}
		reply := TaskReply{}
		ret := callRetry(coordinatorAddress, "Coordinator.GetTask", &args, &reply, retries)
		if !ret {
			log.Printf("Coordinator %v unavailable, exiting worker", coordinatorAddress)
			return
		}
		switch reply.TaskType {
		case MapTask:
			if !handleMapTask(w, reply, mapf) {
				mapReportRet := reportTaskFailure(w, reply.TaskId, MapTask, nil)
				if !mapReportRet {
					return
				}
			} else {
				mapNotifyRet := notifyCoordinator(w, MapTask, reply)
				if !mapNotifyRet {
					return
				}
			}
		case ReduceTask:
			retHandleReduce, failedMaps := handleReduceTask(reply, reducef)
			if !retHandleReduce {
				reduceReportRet := reportTaskFailure(w, reply.TaskId, ReduceTask, failedMaps)
				if !reduceReportRet {
					return
				}
			} else {
				reduceNotifyRet := notifyCoordinator(w, ReduceTask, reply)
				if !reduceNotifyRet {
					return
				}
			}
		case NoAvailableTask:
			time.Sleep(time.Second)
		case Exit:
			return
		}
	}
}

func handleMapTask(w *WorkerStruct, reply TaskReply, mapf func(string, string) []KeyValue) bool {
	filename := reply.File
	file, err := os.Open(filename)
	if err != nil {
		log.Printf("cannot open %v", filename)
		return false
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Printf("cannot read %v", filename)
		return false
	}
	file.Close()
	kva := mapf(filename, string(content))
	buckets := make([][]KeyValue, reply.NReduce)
	for _, kv := range kva {
		h := ihash(kv.Key) % reply.NReduce
		buckets[h] = append(buckets[h], kv)
	}
	return writeIntermediateFiles(buckets, reply)
}

func notifyCoordinator(w *WorkerStruct, taskType TaskType, reply TaskReply) bool {
	doneArgs := TaskDoneArgs{
		TaskId:        reply.TaskId,
		TaskType:      taskType,
		WorkerAddress: w.WorkerAddress,
	}
	ret := callRetry(w.CoordinatorAddress, "Coordinator.SetTaskDone", &doneArgs, &TaskDoneReply{}, retries)
	if !ret {
		log.Printf("cannot connect to coordinator %v", w.CoordinatorAddress)
		return false
	}
	return true
}

func reportTaskFailure(w *WorkerStruct, taskId int, taskType TaskType, failedMaps map[int]string) bool {
	args := TaskFailedArgs{TaskId: taskId, TaskType: taskType, WorkerAddr: w.WorkerAddress, FailedMaps: failedMaps}
	reply := TaskFailedReply{}
	ret := callRetry(w.CoordinatorAddress, "Coordinator.SetTaskFailed", &args, &reply, retries)
	if !ret {
		log.Printf("cannot connect to coordinator %v", w.CoordinatorAddress)
		return false
	}
	return true
}

func readIntermediateFiles(reply TaskReply) ([]KeyValue, map[int]string) {
	intermediate := []KeyValue{}
	failedMaps := make(map[int]string)
	for mapId, addr := range reply.MapsAddr {
		filename := fmt.Sprintf("mr-%d-%d", mapId, reply.TaskId)
		getArgs := GetIntermediateArgs{
			Filename: filename,
		}
		getReply := GetIntermediateReply{}
		ret := callRetry(addr, "WorkerStruct.GetIntermediate", &getArgs, &getReply, retries)
		if ret {
			intermediate = append(intermediate, getReply.Intermediate...)
		} else {
			log.Printf("cannot call worker %s", addr)
			failedMaps[mapId] = addr
		}
	}
	return intermediate, failedMaps
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

func writeIntermediateFiles(buckets [][]KeyValue, reply TaskReply) bool {
	for i := 0; i < reply.NReduce; i++ {
		oname := fmt.Sprintf("mr-%d-%d", reply.TaskId, i)
		ofile, tempErr := ioutil.TempFile("", "mr-tmp-*")
		if tempErr != nil {
			log.Printf("TempFile error:", tempErr)
			return false
		}
		enc := json.NewEncoder(ofile)
		for _, kv := range buckets[i] {
			encodeErr := enc.Encode(&kv)
			if encodeErr != nil {
				log.Printf("Encode error:", encodeErr)
				return false
			}
		}
		ofile.Close()
		/*
			err := os.Rename(ofile.Name(), oname)
			if err != nil {
				log.Fatal("os.Rename error:", err)
			}
		*/
		//I chose to use MoveFile even if rename is better bcs when
		//I tried to run the program on remote machine, I had the errors: invalid cross-device link
		//Bcs TempFile store temp files in /tmp which can be in a different partition
		//and then os.rename had problems to access different partitions
		//error also report here: https://gist.github.com/var23rav/23ae5d0d4d830aff886c3c970b8f6c6b
		moveFileErr := MoveFile(ofile.Name(), oname)
		if moveFileErr != nil {
			log.Printf("MoveFile error:", moveFileErr)
			return false
		}
	}
	return true
}

func handleReduceTask(reply TaskReply, reducef func(string, []string) string) (bool, map[int]string) {
	intermediate, failedMaps := readIntermediateFiles(reply)
	if len(failedMaps) > 0 {
		return false, failedMaps
	}
	sort.Sort(ByKey(intermediate))
	oname := fmt.Sprintf("mr-out-%d", reply.TaskId)
	ofile, tempErr := ioutil.TempFile("", "mr-tmp-out-*")
	if tempErr != nil {
		log.Printf("TempFile error:", tempErr)
		return false, nil
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
	/*
		err := os.Rename(ofile.Name(), oname)
		if err != nil {
			log.Fatal("os.Rename error:", err)
		}
	*/
	//I chose to use MoveFile even if rename is better bcs when
	//I tried to run the program on remote machine, I had the errors: invalid cross-device link
	//Bcs TempFile store temp files in /tmp which can be in a different partition
	//and then os.rename had problems to access different partitions
	//error also report here: https://gist.github.com/var23rav/23ae5d0d4d830aff886c3c970b8f6c6b
	moveFileErr := MoveFile(ofile.Name(), oname)
	if moveFileErr != nil {
		log.Printf("MoveFile error:", moveFileErr)
		return false, nil
	}
	return true, nil
}

func (w *WorkerStruct) server() {
	rpc.Register(w)
	rpc.HandleHTTP()
	l, err := net.Listen("tcp", w.WorkerAddress)
	if err != nil {
		log.Fatalf("Worker RPC listen error: %v", err)
	}
	w.Listener = l
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
		log.Printf("dialing:", err)
		return false
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

func callRetry(address string, rpcname string, args interface{}, reply interface{}, retries int) bool {
	for i := 0; i < retries; i++ {
		if call(address, rpcname, args, reply) {
			return true
		}
		time.Sleep(time.Second)
	}
	return false
}

func closeListener(w *WorkerStruct) {
	if w.Listener != nil {
		w.Listener.Close()
	}
}

func (w *WorkerStruct) Heartbeat(args *HeartbeatArgs, reply *HeartbeatReply) error {
	return nil
}
