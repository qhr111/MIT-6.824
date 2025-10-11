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
	"strconv"
)

type Coordinator struct {
	// 锁
	lock sync.Mutex

	stage          string //当前阶段, MAP or REDUCE, 为空代表已完成可退出
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
	if c.stage == MAP && len(c.tasks) == 0 {
		//进入REDUCE阶段
		c.stage = REDUCE
		//生成REDUCE任务放入任务池
		//先合并相同key的intermediate文件
		intermediate := []mr.KeyValue{}
		for _, filename := range os.Args[2:] {
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
			intermediate = append(intermediate, kva...) //...表示把kva切片的所有元素添加到intermediate切片中
		}

		//
		// a big difference from real MapReduce is that all the
		// intermediate data is in one place, intermediate[],
		// rather than being partitioned into NxM buckets.
		//

		sort.Sort(ByKey(intermediate))

		oname := "mr-out-0"
		ofile, _ := os.Create(oname)

		//
		// call Reduce on each distinct key in intermediate[],
		// and print the result to mr-out-0.
		//
		i := 0
		for i < len(intermediate) {
			j := i + 1
			for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
				j++
			}
			values := []string{} //{}表示初始化为空切片
			// 把相同key的value都放到values切片中
			for k := i; k < j; k++ {
				values = append(values, intermediate[k].Value)
			}

func (c *Coordinator) getTask(args *ApplyForArgs, reply *ApplyForReply) error {
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
	if c.stage == "REDUCE" && len(c.tasks) == 0 {
		ret = true
	}
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
