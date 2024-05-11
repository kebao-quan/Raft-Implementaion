package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

// Add your RPC definitions here.

// Status
// 0: Worker is asking for a job
// 1: Worker is reporting a finished map task
type Args struct {
	Status int
	TaskID int
}

// Jobs
// 0: nothing left to work on
// 1: map task
// 2: reduce task
type Reply struct {
	NReduce  int
	Jobs     int
	FileName string
	TaskID   int
	Key      string
	Value    []string
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
