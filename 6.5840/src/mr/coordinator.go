package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"sync"
	"time"
)

const WorkerTimeout = 10 * time.Second
const HeartBeatInterval = 2 * time.Second
const MaxHeartBeatFailed = 3

type TaskType int

const (
	MapTask TaskType = iota
	ReduceTask
	NoAvailableTask
	Exit
)

type TaskStatus int

const (
	Idle TaskStatus = iota
	InProgress
	Completed
)

type Task struct {
	id         int
	filename   string
	status     TaskStatus
	startTime  time.Time
	workerAddr string
}

type WorkerInfo struct {
	addr             string
	lastAnswer       time.Time
	missedHeartbeats int
}

type Coordinator struct {
	mutex       sync.Mutex
	mapTasks    []Task
	reduceTasks []Task
	nReduce     int
	nMap        int
	mapDone     bool
	reduceDone  bool
	mapsAddr    map[int]string
	workers     map[string]*WorkerInfo
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) GetTask(args *TaskArgs, reply *TaskReply) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if args.WorkerAddress != "" {
		resetTimeWorker(c, args.WorkerAddress)
	}
	mapTasksReply := handleTask(c, args, reply, c.mapTasks, MapTask)
	if mapTasksReply {
		return nil
	}
	if c.mapDone {
		reduceTasksReply := handleTask(c, args, reply, c.reduceTasks, ReduceTask)
		if reduceTasksReply {
			return nil
		}
	}
	if c.mapDone && c.reduceDone {
		reply.TaskType = Exit
		return nil
	}
	reply.TaskType = NoAvailableTask
	return nil
}

func checkAllTasksCompleted(c *Coordinator) {
	checkAllTypeTasksCompleted(c, c.mapTasks, MapTask)
	checkAllTypeTasksCompleted(c, c.reduceTasks, ReduceTask)
}

func checkAllTypeTasksCompleted(c *Coordinator, tasks []Task, taskType TaskType) {
	tasksCompleted := true
	for _, task := range tasks {
		if task.status != Completed {
			tasksCompleted = false
			break
		}
	}
	if taskType == MapTask {
		c.mapDone = tasksCompleted
	} else {
		c.reduceDone = tasksCompleted
	}
}

func handleTask(c *Coordinator, args *TaskArgs, reply *TaskReply, tasks []Task, taskType TaskType) bool {
	hasReply := handleReply(c, args, reply, tasks, taskType)
	checkAllTypeTasksCompleted(c, tasks, taskType)
	return hasReply
}

func handleReply(c *Coordinator, args *TaskArgs, reply *TaskReply, tasks []Task, taskType TaskType) bool {
	for i := range tasks {
		task := &tasks[i]
		if task.status == Idle || (task.status == InProgress && time.Since(task.startTime) > WorkerTimeout) {
			task.status = InProgress
			task.startTime = time.Now()
			task.workerAddr = args.WorkerAddress
			reply.TaskType = taskType
			reply.TaskId = i
			reply.NReduce = c.nReduce
			reply.NMap = c.nMap
			if taskType == MapTask {
				reply.File = task.filename
				reply.MapsAddr = nil
			} else {
				reply.File = ""
				reply.MapsAddr = make(map[int]string)
				for k, v := range c.mapsAddr {
					reply.MapsAddr[k] = v
				}
			}
			return true
		}
	}
	return false
}

func (c *Coordinator) SetTaskDone(args *TaskDoneArgs, reply *TaskDoneReply) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	resetTimeWorker(c, args.WorkerAddress)
	switch args.TaskType {
	case MapTask:
		c.mapTasks[args.TaskId].status = Completed
		c.mapTasks[args.TaskId].workerAddr = args.WorkerAddress
		c.mapsAddr[args.TaskId] = args.WorkerAddress
	case ReduceTask:
		c.reduceTasks[args.TaskId].status = Completed
		c.reduceTasks[args.TaskId].workerAddr = args.WorkerAddress
	}
	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) SetTaskFailed(args *TaskFailedArgs, reply *TaskFailedReply) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	for _, addr := range args.FailedMaps {
		resetTask(c, addr, c.mapTasks, MapTask)
	}
	switch args.TaskType {
	case MapTask:
		resetTask(c, args.WorkerAddr, c.mapTasks, MapTask)
	case ReduceTask:
		resetTask(c, args.WorkerAddr, c.reduceTasks, ReduceTask)
	}
	checkAllTasksCompleted(c)
	return nil
}

func (c *Coordinator) heartBeatWorkers() {
	ticker := time.NewTicker(HeartBeatInterval)
	defer ticker.Stop()
	for range ticker.C {
		c.mutex.Lock()
		addrs := make([]string, 0, len(c.workers))
		for addr := range c.workers {
			addrs = append(addrs, addr)
		}
		c.mutex.Unlock()
		for _, addr := range addrs {
			ret := callWorkerHeartbeat(addr)
			if !ret {
				handleWorkerFailure(c, addr)
			} else {
				updateLastAnswer(c, addr)
			}
		}
	}
}

func updateLastAnswer(c *Coordinator, addr string) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	resetTimeWorker(c, addr)
}

func resetTimeWorker(c *Coordinator, WorkerAddress string) {
	w, found := c.workers[WorkerAddress]
	if found {
		w.lastAnswer = time.Now()
		w.missedHeartbeats = 0
	} else {
		c.workers[WorkerAddress] = &WorkerInfo{
			addr:             WorkerAddress,
			lastAnswer:       time.Now(),
			missedHeartbeats: 0,
		}
	}
}

func callWorkerHeartbeat(workerAddr string) bool {
	args := HeartbeatArgs{}
	reply := HeartbeatReply{}
	c, dialErr := rpc.DialHTTP("tcp", workerAddr)
	if dialErr != nil {
		return false
	}
	defer c.Close()
	callErr := c.Call("WorkerStruct.Heartbeat", &args, &reply)
	return callErr == nil
}

func handleWorkerFailure(c *Coordinator, addr string) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	w, found := c.workers[addr]
	if !found {
		return
	}
	w.missedHeartbeats++
	if w.missedHeartbeats != MaxHeartBeatFailed {
		return
	}
	log.Printf("Coordinator: worker %s failed (no heartbeat). Resetting its tasks.", addr)
	resetTask(c, addr, c.mapTasks, MapTask)
	resetTask(c, addr, c.reduceTasks, ReduceTask)
	checkAllTasksCompleted(c)
}

func resetTask(c *Coordinator, addr string, tasks []Task, taskType TaskType) {
	for i := range tasks {
		task := &tasks[i]
		if task.workerAddr == addr {
			task.status = Idle
			task.startTime = time.Time{}
			task.workerAddr = ""
			if taskType == MapTask {
				delete(c.mapsAddr, task.id)
			}
		}
	}
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server(address string) {
	rpc.Register(c)
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", address)
	//sockname := coordinatorSock()
	//os.Remove(sockname)
	//l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	return c.mapDone && c.reduceDone
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(address string, files []string, nReduce int) *Coordinator {
	c := Coordinator{
		mapTasks:    make([]Task, len(files)),
		reduceTasks: make([]Task, nReduce),
		nReduce:     nReduce,
		nMap:        len(files),
		mapsAddr:    make(map[int]string),
		workers:     make(map[string]*WorkerInfo),
	}
	for i, file := range files {
		c.mapTasks[i] = Task{
			id:       i,
			filename: file,
			status:   Idle,
		}
	}
	for i := 0; i < nReduce; i++ {
		c.reduceTasks[i] = Task{
			id:     i,
			status: Idle,
		}
	}
	c.server(address)
	go c.heartBeatWorkers()
	return &c
}
