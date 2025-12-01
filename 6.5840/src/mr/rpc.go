package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
type TaskArgs struct {
	WorkerAddress string
}

type TaskReply struct {
	TaskId   int
	TaskType TaskType
	File     string
	NReduce  int
	NMap     int
	MapsAddr map[int]string
}

type TaskDoneArgs struct {
	TaskId        int
	TaskType      TaskType
	WorkerAddress string
}

type TaskDoneReply struct {
}

type GetIntermediateArgs struct {
	Filename string
}

type GetIntermediateReply struct {
	Intermediate []KeyValue
}

type HeartbeatArgs struct{}

type HeartbeatReply struct {
	Alive bool
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
