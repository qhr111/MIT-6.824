package mr

import (
	"fmt"
	"os"
)
import "log"
import "net/rpc"
import "hash/fnv"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
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

	var LastTaskType string
	var LastTaskIndex int
	for {
		args := ApplyForArgs{
			WorkerID: id,
			LastTaskType LastTaskType,
			LastTaskIndex LastTaskIndex,
		}

		reply := ArgsForReply{}
		call("Coordinator.ApplyHandler", &args, &reply)

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
			
		}
		LastTaskType = reply.TaskType
		LastTaskIndex = reply.TaskIndex
		log.Printf("Finished %s task %d", reply.TaskType,reply.TaskIndex)

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
