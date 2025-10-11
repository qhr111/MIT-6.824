package mr

import (
	"fmt"
	"os"
	"strconv"
	"log"
	"net/rpc"
	"hash/fnv"
	"io/ioutil"
	"strings"
	"sort"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	// 单机运行，直接使用 PID 作为 Worker ID，方便 debug
	id := strconv.Itoa(os.Getpid())
	log.Printf("Worker %s started\n", id)

	// 进入循环，向 Coordinator 申请 Task
	var lastTaskType string
	var lastTaskIndex int
	for {
		args := ApplyForTaskArgs{
			WorkerID:      id,
			LastTaskType:  lastTaskType,
			LastTaskIndex: lastTaskIndex,
		}
		reply := ApplyForTaskReply{}
		call("Coordinator.ApplyForTask", &args, &reply)

		if reply.TaskType == "" {
			// MR 作业已完成，退出
			log.Printf("Received job finish signal from coordinator")
			break
		}

		log.Printf("Received %s task %d from coordinator", reply.TaskType, reply.TaskIndex)
		if reply.TaskType == MAP {
			// 读取输入数据
			file, err := os.Open(reply.MapInputfile)
			if err != nil {
				log.Fatalf("Failed to open map input file %s: %e", reply.MapInputfile, err)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("Failed to read map input file %s: %e", reply.MapInputfile, err)
			}
			// 传递输入数据至 MAP 函数，得到中间结果
			kva := mapf(reply.MapInputfile, string(content))
			// 按 Key 的 Hash 值对中间结果进行分桶
			hashedKva := make(map[int][]KeyValue)
			for _, kv := range kva {
				hashed := ihash(kv.Key) % reply.ReduceNum
				hashedKva[hashed] = append(hashedKva[hashed], kv)
			}
			// 写出中间结果文件
			for i := 0; i < reply.ReduceNum; i++ {
				ofile, _ := os.Create(tmpMapOutFile(id, reply.TaskIndex, i))
				for _, kv := range hashedKva[i] {
					fmt.Fprintf(ofile, "%v\t%v\n", kv.Key, kv.Value)
				}
				ofile.Close()
			}
		} else if reply.TaskType == REDUCE {
			// 读取输入数据
			var lines []string
			for mi := 0; mi < reply.MapNum; mi++ {
				inputFile := finalMapOutFile(mi, reply.TaskIndex)
				file, err := os.Open(inputFile)
				if err != nil {
					log.Fatalf("Failed to open map output file %s: %e", inputFile, err)
				}
				content, err := ioutil.ReadAll(file)
				if err != nil {
					log.Fatalf("Failed to read map output file %s: %e", inputFile, err)
				}
				lines = append(lines, strings.Split(string(content), "\n")...)
			}
			var kva []KeyValue
			for _, line := range lines {
				if strings.TrimSpace(line) == "" {
					continue
				}
				parts := strings.Split(line, "\t")
				kva = append(kva, KeyValue{
					Key: parts[0],
					Value: parts[1],
				})
			}
			sort.Sort(ByKey(kva))

			ofile, _ := os.Create(tmpReduceOutFile(id, reply.TaskIndex))
			i := 0
			for i < len(kva) {
				j := i + 1
				for j < len(kva) && kva[j].Key == kva[i].Key {
					j++
				}
				var values []string
				for k := i; k < j; k++ {
					values = append(values, kva[k].Value)
				}
				output := reducef(kva[i].Key, values)

				fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

				i = j
			}
			ofile.Close()
		}
		lastTaskType = reply.TaskType
		lastTaskIndex = reply.TaskIndex
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
