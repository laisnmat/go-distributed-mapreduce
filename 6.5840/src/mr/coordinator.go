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
const CheckInterval = 2 * time.Second

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
	Id         int
	File       string
	Status     TaskStatus
	StartTime  time.Time
	WorkerAddr string
}

type WorkerInfo struct {
	Addr     string
	Alive    bool
	LastSeen time.Time
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
		if w, ok := c.workers[args.WorkerAddress]; ok {
			w.LastSeen = time.Now()
			w.Alive = true
		} else {
			c.workers[args.WorkerAddress] = &WorkerInfo{
				Addr:     args.WorkerAddress,
				Alive:    true,
				LastSeen: time.Now(),
			}
		}
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

func handleTask(c *Coordinator, args *TaskArgs, reply *TaskReply, tasks []Task, taskType TaskType) bool {
	hasReply := handleReply(c, args, reply, tasks, taskType)
	tasksCompleted := true
	for _, task := range tasks {
		if task.Status != Completed {
			tasksCompleted = false
			break
		}
	}
	if taskType == MapTask {
		c.mapDone = tasksCompleted
	} else {
		c.reduceDone = tasksCompleted
	}
	return hasReply
}

func handleReply(c *Coordinator, args *TaskArgs, reply *TaskReply, tasks []Task, taskType TaskType) bool {
	for i := range tasks {
		task := &tasks[i]
		if task.Status == Idle || (task.Status == InProgress && time.Since(task.StartTime) > WorkerTimeout) {
			task.Status = InProgress
			task.StartTime = time.Now()
			task.WorkerAddr = args.WorkerAddress
			reply.TaskType = taskType
			reply.TaskId = i
			reply.NReduce = c.nReduce
			reply.NMap = c.nMap
			if taskType == MapTask {
				reply.File = task.File
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
	switch args.TaskType {
	case MapTask:
		c.mapTasks[args.TaskId].Status = Completed
		c.mapTasks[args.TaskId].WorkerAddr = args.WorkerAddress
		c.mapsAddr[args.TaskId] = args.WorkerAddress
	case ReduceTask:
		c.reduceTasks[args.TaskId].Status = Completed
		c.reduceTasks[args.TaskId].WorkerAddr = args.WorkerAddress
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

func (c *Coordinator) monitorWorkers() {
	ticker := time.NewTicker(CheckInterval)
	for range ticker.C {
		c.mutex.Lock()
		addrs := make([]string, 0, len(c.workers))
		for addr := range c.workers {
			addrs = append(addrs, addr)
		}
		c.mutex.Unlock()

		for _, addr := range addrs {
			ok := callWorkerHeartbeat(addr)
			if !ok {
				c.handleWorkerFailure(addr)
			} else {
				c.mutex.Lock()
				if w, ok := c.workers[addr]; ok {
					w.LastSeen = time.Now()
					w.Alive = true
				} else {
					c.workers[addr] = &WorkerInfo{
						Addr:     addr,
						Alive:    true,
						LastSeen: time.Now(),
					}
				}
				c.mutex.Unlock()
			}
		}
	}
}

func callWorkerHeartbeat(workerAddr string) bool {
	args := HeartbeatArgs{}
	reply := HeartbeatReply{}
	c, err := rpc.DialHTTP("tcp", workerAddr)
	if err != nil {
		return false
	}
	defer c.Close()
	if err := c.Call("Worker.Heartbeat", &args, &reply); err != nil {
		return false
	}
	return reply.Alive
}

func (c *Coordinator) handleWorkerFailure(addr string) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	w, ok := c.workers[addr]
	if !ok || !w.Alive {
		return
	}
	log.Printf("Coordinator: worker %s failed (no heartbeat). Resetting its tasks.", addr)
	w.Alive = false
	for i := range c.mapTasks {
		task := &c.mapTasks[i]
		if task.WorkerAddr == addr {
			task.Status = Idle
			task.StartTime = time.Time{}
			task.WorkerAddr = ""
			delete(c.mapsAddr, task.Id)
		}
	}
	for i := range c.reduceTasks {
		task := &c.reduceTasks[i]
		if task.WorkerAddr == addr && task.Status == InProgress {
			task.Status = Idle
			task.StartTime = time.Time{}
			task.WorkerAddr = ""
		}
	}
	allMapsDone := true
	for _, mt := range c.mapTasks {
		if mt.Status != Completed {
			allMapsDone = false
			break
		}
	}
	c.mapDone = allMapsDone
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
			Id:     i,
			File:   file,
			Status: Idle,
		}
	}
	for i := 0; i < nReduce; i++ {
		c.reduceTasks[i] = Task{
			Id:     i,
			Status: Idle,
		}
	}
	c.server(address)
	go c.monitorWorkers()
	return &c
}
