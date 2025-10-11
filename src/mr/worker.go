package mr

import (
	"fmt"
	"os"
	"strconv"
	"log"
	"net/rpc"
	"hash/fnv"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type Worker struct{
	WorkerID int
	TaskID int
}

type Task struct{
	TaskType string //MAP or REDUCE
	TaskID int
	MapInputfile string //map任务的输入文件名

	WorkerID int //分配给哪个worker
	Deadline time.Time //任务截止时间
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	id := strconv.Itoa(os.Getpid())
	log.Printf("Worker %s started\n", id)

	for {
		args := getTaskArgs{
			WorkerID: id,
		}

		reply := getTaskReply{}
		call("Coordinator.getTask", &args, &reply)

		if reply.TaskType == ""{
			// MR作用已完成,退出
			log.Printf("Received job finish signal from coordinator")
			break
		}else if reply.TaskType == MAP{
			//生成的Map的kv要用ihash来分桶给不同的reduce用!
			file, err = os.Open(reply.MapInputfile)
			if err != nil {
				log.Fatal("Failed to open map input file %s : %e", reply.MapInputfile, err)
			}
			content, err := os.ReadAll(file)
			if err != nil {
				log.Fatal("Failed to read map input file %s : %e", reply.MapInputfile, err)
			}
			hashedKva := make(map[int][]KeyValue)
			kva := mapf(reply.MapInputfile, string(content))
			//将中间结果分配到不同的桶中
			for _, kv := range kva{
				hashed := ihash(kv.key) % reply.ReduceNum
				hashedKva[hashed] = append(hashedKva, kv)
			}

			//写出中间结果
			for i := 0; i < reply.ReduceNum; i++ {
				ofile, _ := os.Create(tmpMapOutFile(id, reply.TaskIndex, i))
				for _, kv := range hashedKva[i] {
					fmt.Fprintf(ofile, "%v\t%v\n", kv.Key, kv.Value)	
				}
				ofile.close()
			}
			
		}else if reply.TaskType == REDUCE{
			//执行REDUCE任务	
			file, err = os.Open(reply.MapInputfile)
			if err != nil {
				log.Fatal("Failed to open reduce input file %s : %e", reply.MapInputfile, err)
			}
			content, err := os.ReadAll(file)
			if err != nil {
				log.Fatal("Failed to read reduce input file %s : %e", reply.MapInputfile, err)
			}

			//从content中解析出key， 并还原出values
			values := []string{}
			lines := strings.Split(string(content), "\n")
			key := lines[0].split("\t")[0]

			for _, line := range lines{
				kv := strings.Split(line, "\t")
				if len(kv) != 2{
					continue
				}
				values = append(values, kv[1])
			}
			//values 生成好了，可以执行reduce函数
			//这里是一个值
			output := reducef(key, values)

			//写入reduce 临时输出函数
			ofile := os.Create(tmpReduceOutFile(id, reply.TaskIndex))
			fmt.Fprintf(ofile, "%v %v\n", key, output)	
			ofile.close()
		
		}
		
		//告诉coordinator 这个任务完成了
		//TODO: 失败重试机制， 如何告诉coordintor存储的地址?事先商量好？
		finishArgs := finishArgs{
			TaskType: reply.TaskType,
			TaskID: reply.TaskIndex,
		}
		finishReply := finishReply{}
		call("Coordinator.finishTask", &finishArgs, &finishReply)
		if finishReply.msg != OK{
			log.Printf("Worker %s failed to report finish of task %d", id, reply.TaskIndex)
			break
		}

		log.Printf("Finished %s task %d", reply.TaskType, reply.TaskIndex)

	}
	log.Printf("Worker %s exit\n", id)
}

// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
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
