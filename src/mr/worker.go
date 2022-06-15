package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"time"
)

const TaskInterval = 200

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	n, succ := getReduceCount()
	if succ == false {
		fmt.Println("Failed to get reduce task count, worker exiting.")
		return
	}
	nReduce := n
	for {
		task := getTask()
		
		switch task.Type {
		case NoTask:
			break
		case ExitTask:
			return
		case MapTask:
			performMapTask(task, nReduce, mapf)
		case ReduceTask:
			performReduceTask(task, reducef)
		}

		time.Sleep(time.Millisecond * TaskInterval)
	}
}

func performMapTask(task Task, reduceCount int, mapf func(string, string) []KeyValue) {
	// Read the task file given to us by the coordinator
	file, err := os.Open(task.File)
	if err != nil {
		log.Fatalf("cannot open %v", task.File)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", task.File)
	}
	file.Close()

	// Apply the map function
	kva := mapf(task.File, string(content))

	// Write key-value to each of the nReduce (given by coordinator) files. Which file? Use ihash(key) % nReduce to get a digit between 0 & (nReduce-1)
	files := make([]*os.File, 0, reduceCount)
	encoders := make([]*json.Encoder, 0, reduceCount)

	for i := 0; i < reduceCount; i++ {
		filePath := fmt.Sprintf("temp-mr-%v-%v", task.Index, i)
		file, err := os.Create(filePath)
		if err != nil {
			log.Fatalf("cannot create file %v", filePath)
		}
		files = append(files, file)
		encoders = append(encoders, json.NewEncoder(file))
	}
	for _, kv := range kva {
		key := ihash(kv.Key) % reduceCount
		err := encoders[key].Encode(&kv)
		if err != nil {
			log.Fatalf("cannot encode key, value %v, %v", kv.Key, kv.Value)
		}
	}

	/*
		To ensure that nobody observes partially written files in the presence of crashes, the MapReduce paper mentions the trick of using a temporary file and atomically renaming it once it is completely written.
	*/
	for i, file := range files {
		file.Close()
		newPath := fmt.Sprintf("mr-%v-%v", task.Index, i)
		err := os.Rename(file.Name(), newPath)
		if err != nil {
			log.Fatalf("Cannot rename file %v", file.Name())
		}
	}

	completeMapTask(task)
}

func performReduceTask(task Task, reducef func(string, []string) string) {
	// Get all intermediate file paths. We know the format bc we wrote them in performMapTask
	filepaths, err := filepath.Glob(fmt.Sprintf("mr-%v-%v", "*", task.Index))
	if err != nil {
		log.Fatalf("cannot glob")
	}

	// Reconstruct the key-value map
	kvMap := make(map[string][]string)
	for _,filepath := range filepaths {
		file, err := os.Open(filepath)
		if err != nil {
			log.Fatalf("cannot open %v", task.File)
		}

		dec := json.NewDecoder(file)
		
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kvMap[kv.Key] = append(kvMap[kv.Key], kv.Value)
		}
	}
	
	// Sort our keys
	keys := make([]string, 0, len(kvMap))
	for k := range kvMap {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	// Apply the reduce function
	filePath := fmt.Sprintf("temp-mr-out-%v", task.Index)
	file, err := os.Create(filePath)
	if err != nil {
		log.Fatalf("cannot create file %v", filePath)
	}
	for _,key := range keys {
		output := reducef(key, kvMap[key])
		_, err := fmt.Fprintf(file, "%v %v\n", key, output)
		if err != nil {
			log.Fatalf("cannot write to file %v", file)
		}	
	}

	// Rename the file to atomically write like we did in performMapTask
	file.Close()
	newPath := fmt.Sprintf("mr-out-%v", task.Index)
	err = os.Rename(filePath, newPath)
	if err != nil {
		log.Fatalf("Cannot rename file %v", filePath)
	}
	
	completeReduceTask(task)
}

func getTask() Task {
	args := GetTaskArgs{}
	reply := Task{}
	call("Coordinator.GetNextTask", &args, &reply)
	return reply
}

func completeMapTask(task Task) {
	reply := CompleteTaskReply{}
	call("Coordinator.CompleteMapTask", &task, &reply)
}

func getReduceCount() (int, bool) {
	args := GetReduceCountArgs{}
	reply := GetReduceCountReply{}
	succ := call("Coordinator.GetReduceCount", &args, &reply)
	return reply.ReduceCount, succ
}

func completeReduceTask(task Task) {
	reply := CompleteTaskReply{}
	call("Coordinator.CompleteReduceTask", &task, &reply)
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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
