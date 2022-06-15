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

type GetTaskArgs struct {
}

type CompleteTaskReply struct {
}

// Add your RPC definitions here.

type MapJob struct { 
	InputFile    string 
	MapJobNumber int 
	ReducerCount int
}

type ReduceJob struct { 
	IntermediateFiles []string 
	ReduceNumber      int
}

type RequestTaskReply struct { 
	MapJob    *MapJob 
	ReduceJob *ReduceJob 
	Done      bool
}

type ReportMapTaskArgs struct { 
	InputFile        string
	IntermediateFile []string
}

type GetReduceCountArgs struct {
}

type GetReduceCountReply struct {
	ReduceCount int
}


// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
