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
	Id        int
	File      string
	Status    TaskStatus
	StartTime time.Time
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
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) GetTask(args *TaskArgs, reply *TaskReply) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	mapTasksReply := handleTask(c, reply, c.mapTasks, MapTask)
	if mapTasksReply {
		return nil
	}
	if c.mapDone {
		reduceTasksReply := handleTask(c, reply, c.reduceTasks, ReduceTask)
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

func handleTask(c *Coordinator, reply *TaskReply, tasks []Task, taskType TaskType) bool {
	hasReply := handleReply(c, reply, tasks, taskType)
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

func handleReply(c *Coordinator, reply *TaskReply, tasks []Task, taskType TaskType) bool {
	for i := range tasks {
		task := &tasks[i]
		if task.Status == Idle || (task.Status == InProgress && time.Since(task.StartTime) > WorkerTimeout) {
			task.Status = InProgress
			task.StartTime = time.Now()
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
		c.mapsAddr[args.TaskId] = args.WorkerAddress
	case ReduceTask:
		c.reduceTasks[args.TaskId].Status = Completed
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
	return &c
}
