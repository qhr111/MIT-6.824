package mr

import (
	"fmt"
	"log"
	"math"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	// 锁
	lock sync.Mutex

	stage          string //当前阶段, MAP or REDUCE, 为空代表已完成可退出
	nMap           int    // map任务数
	nReduce        int    // reduce任务数
	tasks          map[string]Task
	availableTasks chan Task
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
// 这里的c *Coordinator表示这个方法属于Coordinator类型
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) ApplyHandler(args *ApplyForArgs, reply *ApplyForReply) error {
	//如何标识这个task完成了?
	if args.LastTaskType != "" {
		// 记录Worker的上一个Task已经运行完成
		c.lock.Lock()

		lastTaskID = GenTaskID(args.LastTaskType, args.LastTaskIndex)

		if task, exists := c.tasks[lastTaskID]; exists && task.WorkerID == args.WorkerID {
			//判断该Task 是否仍属于该Worker, 如果已经被重新分配, 进入后续的新task分配过程(因为可能超时了)
			if args.LastTaskType == MAP {
				//如何处理MAP
			} else if args.LastTaskType == REDUCE {
				//如何处理REDUCE
			}

			delete(c.tasks, lastTaskID)
			if len(c.tasks) == 0 {
				c.transit()
			}
		}
		c.loc.Unlock()
	}
	//获取一个可用的task并返回
	task, ok := <-availableTasks
	if !ok { //chanle关闭,MAP任务已完成,  通知worker退出
		log.Printf("channel close!\n")
		return nil
	}
	c.lock.Lock()
	defer c.lock.Unlock()

	task.WorkerID = args.WorkerID
	//Add什么意思?
	task.Deadline = time.Now().Add(10 * time.Second)
	c.tasks[GenTaskID(task.Type, task.Index)] = task

	reply.TaskType = task.Type
	reply.TaskIndex = task.Index
	reply.MapInputfile = task.MapInputfile
	reply.MapNum = c.nMap
	reply.ReduceNum = c.nReduce

	return nil
}

func (c *Coordinator) MapNotify(request *MapNotifyArgs, reply *MapNotifyReply) error {
	//worker通知这个文件已经处理完了
	c.files_lock.Lock()
	defer c.files_lock.Unlock()
	//找到这个文件对应的下标
	c.files[request.idx] = 1 //标记为已完成
	reply.state = "ok"
	return nil
}

func (c *Coordinator) ReduceHandler(args *ReduceArgs, reply *ReduceReply) error {
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock() //开启一个unix域套接字
	os.Remove(sockname)           //使用前先删除,防止上次运行遗留的文件影响
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {

	ret := false

	// Your code here.

	return ret
}

func GenTaskID(t string, index int) {
	return fmt.Sprintf("%s-%d", t, index)
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		stage:          MAP,
		nMap:           len(files),
		nReduce:        nReduce,
		tasks:          make(map[string]Task),
		availableTasks: make(chan Task, int(math.Max(float64(len(files)), float64(nReduce)))), //缓冲区大小为任务数的最大值
	}
	// Your code here.
	//对每个file生成一个task加入到task池
	for i, file := range files {
		task := Task{
			Type:         MAP,
			Index:        i,
			MapInputfile: file,
		}
		c.tasks[GenTaskID(task.Type, Task.Index)] = task
		c.availableTasks <- task
	}

	log.Printf("coordinator start\n")
	c.server()
	return &c
}
