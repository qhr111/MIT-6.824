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
	nMap		   int
	nReduce		   int
	tasks          map[string]Task //入总任务
	availableTasks chan Task //任务池
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

func (c *Coordinator) transit() {
	if c.stage == MAP {
		log.Printf("All MAP tasks finished, transit to REDUCE stage\n")
		c.stage = REDUCE

		//生成Reduce任务放入任务池
		for i := 0; i < c.nReduce; i++ {
			task := Task{
				Type:  REDUCE,
				Index: i,
			}
			c.tasks[GenTaskID(task.Type, task.Index)] = task
			c.availableTasks <- task
		}	
	}else if c.stage == REDUCE {
		log.Printf("All REDUCE tasks finished, transit to FINISH stage\n")
		c.stage = ""
		close(c.availableTasks) //关闭任务池, 让申请任务的worker退出
	}


}

func (c *Coordinator) ApplyForTask(args *ApplyForTaskArgs, reply *ApplyForTaskReply) error {
	 
	if args.LastTaskType != "" {
		//非第一个请求, 记录上一个请求完成
		c.lock.Lock()
		defer c.lock.Unlock()
		LastTaskType := GenTaskID(args.LastTaskType, args.LastTaskIndex)
		//判断这个task是否已经被重新分配了， 如果是的话则跳过， 进入后续的流程
		if task, exists := c.tasks[LastTaskType]; exists && task.WorkerID == args.WorkerID {
			log.Printf("Mark %s task %v from worker %v as finished\n", args.LastTaskType, args.LastTaskIndex, args.WorkerID)
		}
		if args.LastTaskType == "MAP" {
			//map任务完成
			//将这个task生成的中间产物变成最终产物，这么做的目的是为了反正重传导致的冲突
			for ri := 0; ri < c.nReduce; ri++ {
				//有nReduce个桶，生成了nReduce个中间map文件
				err := os.Rename(tmpMapOutFile(args.WorkerID, args.LastTaskIndex, ri), finalMapOutFile(args.LastTaskIndex, ri))
				if err != nil {
					log.Fatalf("Failed to rename tmp map output file %s to final file %s: %v", tmpMapOutFile(args.WorkerID, args.LastTaskIndex, ri), finalMapOutFile(args.LastTaskIndex, ri), err)
				}
			}
		}else if LastTaskType == "REDUCE" {
			//reduce任务完成
			err := os.Rename(tmpReduceOutFile(args.WorkerID, args.LastTaskIndex), finalReduceOutFile(args.LastTaskIndex))
			if err != nil {
				log.Fatalf("Failed to rename tmp reduce output file %s to final file %s: %v", tmpReduceOutFile(args.WorkerID, args.LastTaskIndex), finalReduceOutFile(args.LastTaskIndex), err)
			}
		}

		//当前阶段所有Task已完成， 进入下一阶段
		delete(c.tasks, LastTaskType)
		if len(c.tasks) == 0 {
			c.transit()
		}

	}

	task, ok := <-c.availableTasks
	if !ok {
		return nil
	}
	c.lock.Lock()
	defer c.lock.Unlock()
	log.Printf("Assign %s task %v to worker %v\n", task.Type, task.Index, args.WorkerID)
	task.WorkerID = args.WorkerID
	task.Deadline = time.Now().Add(10 * time.Second)
	c.tasks[GenTaskID(task.Type, task.Index)] = task //记录Task分配的WorkerID和Deadline
	reply.TaskType = task.Type
	reply.TaskIndex = task.Index
	reply.MapInputfile = task.MapInputfile
	reply.MapNum = c.nMap
	reply.ReduceNum = c.nReduce

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
	if c.stage == "REDUCE" && len(c.tasks) == 0 {
		ret = true
	}
	return ret
}

func GenTaskID(t string, index int) string{
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
		c.tasks[GenTaskID(task.Type, task.Index)] = task
		c.availableTasks <- task
	}

	log.Printf("coordinator start\n")
	c.server()
	
	//启动Task自动回收过程
	go func(){
		for {
			time.Sleep(time.Second * 5)
			c.lock.Lock()
			for _, task := range c.tasks {
				if task.WorkerID != "" && time.Now().After(task.Deadline) {
					//这个任务已经分配给某个worker了, 但是超过截止时间还没完成, 说明这个worker挂了或者死掉了, 需要重新分配这个任务
					log.Printf("Fount time-out %s task %d from worker %s, reassign it\n", task.Type, task.Index, task.WorkerID)
					task.WorkerID = ""
					c.availableTasks <- task
				}
			}
			c.lock.Unlock()
		}
	}()

	return &c
}
