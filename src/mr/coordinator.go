package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

const TaskTimeout = 10

type TaskStatus int
type TaskType int
type JobStage int

const (
	MapTask TaskType = iota
	ReduceTask
	NoTask
	ExitTask
)

const (
	NotStarted TaskStatus = iota
	Executing
	Finished
)

type TaskState struct {
  Status  TaskStatus
	Started int64
}

type Task struct {
	Type     TaskType
	Status   TaskStatus
	Index    int
	File     string
	Started  int64
}
type Coordinator struct {
	// Your definitions here.
	mapStatus					map[string]TaskState
	mapTaskId         int
	reduceStatus      map[int]TaskState
	nReducer          int 
	intermediateFiles map[int][]string 
	mu        			  sync.Mutex
	done  						bool
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) CompleteMapTask(args *Task, reply *CompleteTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if (time.Now().Unix() - c.mapStatus[args.File].Started <= 10) {
		c.mapStatus[args.File] = TaskState {Finished,0}
	}
	return nil
}

func (c *Coordinator) CompleteReduceTask(args *Task, reply *CompleteTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if (time.Now().Unix() - c.reduceStatus[args.Index].Started <= 10) {
		c.reduceStatus[args.Index] = TaskState {Finished,0}
		finished := true
		for _, v := range c.reduceStatus {
			if v.Status != Finished {
				finished = false
			}
		}
		c.done = finished
	}
	return nil
}

func (c *Coordinator) GetNextTask(args *GetTaskArgs, reply *Task) error {
	c.mu.Lock()
	if (c.done) {
		reply.Type = ExitTask
	} else {
		isReadyToReduce := true
		reply.Type = NoTask
		for k,v := range c.mapStatus {
			if (v.Status == NotStarted || 
				 (v.Status == Executing && time.Now().Unix() - v.Started > 10)) {
				c.mapStatus[k] = TaskState {Executing,time.Now().Unix()}
				*reply = Task{MapTask, Executing, c.mapTaskId, k, time.Now().Unix()}
				c.mapTaskId = c.mapTaskId+1
				isReadyToReduce = false
				break
			} else if (v.Status == Executing) {
				isReadyToReduce = false
			}
		}
	
		if (isReadyToReduce) { // only true if all are Finished
			for k,v := range c.reduceStatus {
				if (v.Status == NotStarted || (v.Status == Executing && time.Now().Unix()- v.Started > 10)) {
					c.reduceStatus[k] = TaskState {Executing,time.Now().Unix()}
					*reply = Task{ReduceTask, Executing, k, "", time.Now().Unix()}
					break
				}
			}
		}
	}

	c.mu.Unlock()
	return nil
}

func (c *Coordinator) GetReduceCount(args *GetReduceCountArgs, reply *GetReduceCountReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	reply.ReduceCount = c.nReducer
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.done
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.done = false
	c.nReducer = nReduce
	c.mapStatus = make(map[string]TaskState)
	c.reduceStatus = make(map[int]TaskState)
	for _, file := range files {
		c.mapStatus[file] = TaskState {NotStarted,0}
	}

	for i := 0; i < nReduce; i++ {
		c.reduceStatus[i] = TaskState {NotStarted,0}
	}

	c.server()
	return &c
}
