package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

const (
	MAP    = "MAP"
	REDUCE = "REDUCE"
)

type Task struct {
	Type         string // MAP or REDUCE
	Index        int
	MapInputfile string // map任务的输入文件名

	WorkerID string    //分配给哪个worker
	Deadline time.Time //任务截止时间
}

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
type ApplyForArgs struct {
	WorkerID      string
	LastTaskType  string //上一个任务类型, MAP or REDUCE
	LastTaskIndex int    //上一个任务的索引
}

type ApplyForReply struct {
	TaskType     string //任务类型, MAP or REDUCE, 为空代表没有任务了
	TaskIndex    int    //任务索引
	MapInputfile string //map任务的输入文件名, reduce任务为空
	MapNum       int
	ReduceNum    int
}

func tmpMapOutFile(worker string, mapIndex int, reduceIndex int) {
	return fmt.Sprintf("tmp-worker-%s-%d-%d", worker, mapIndex, reduceIndex)
}

func finalMapOutFile(mapIndex int, reduceIndex int) string {
	return fmt.Sprintf("mr-%d-%d", mapIndex, reduceIndex)
}

func tmpReduceOutFile(worker string, reduceIndex int) string {
	return fmt.Sprintf("tmp-worker-%s-out-%d", worker, reduceIndex)
}

func finalReduceOutFile(reduceIndex int) string {
	return fmt.Sprintf("mr-out-%d", reduceIndex)
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
