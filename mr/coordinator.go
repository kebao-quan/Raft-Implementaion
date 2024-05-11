package mr

import (
	//"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

const MapTask = 1
const ReduceTask = 2

type Queue struct { //For map tasks
	items []int //TaskID
}

func (q *Queue) Push(item int) {
	q.items = append(q.items, item)
}

func (q *Queue) Pop() int {
	if q.IsEmpty() {
		panic("Queue is empty")
	}
	removedItem := q.items[0]
	q.items = q.items[1:]
	return removedItem
}

func (q *Queue) Peek() int {
	if q.IsEmpty() {
		panic("Queue is empty")
	}
	return q.items[0]
}

func (q *Queue) IsEmpty() bool {
	return len(q.items) == 0
}

func (q *Queue) Size() int {
	return len(q.items)
}

type Task struct {
	TaskID   int
	FileName string
}

type TaskStatus struct {
	TaskID   int
	TaskType int
	Assigned time.Time
}

type Coordinator struct {
	AllTasks   []Task
	MapQueue   Queue
	ReduceQue  Queue
	NReduce    int
	MapDone    bool
	ReduceDone bool
	mu         sync.Mutex
	TaskTrack  map[int]*TaskStatus
}

// RPC definitions.
func (c *Coordinator) Scheduler(args *Args, reply *Reply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.ReduceDone {
		reply.Jobs = -1
		return nil
	}

	c.CheckTimeouts() //Check if any tasks have timed out and reassign them

	if !c.MapDone { //Map phase
		if c.MapQueue.IsEmpty() && len(c.TaskTrack) == 0 { //No more tasks to assign
			//fmt.Println("Map phase completed")
			c.MapDone = true
		} else {
			if args.Status == 0 { // If the worker is asking for a job, then
				if c.MapQueue.IsEmpty() {
					reply.Jobs = 0
				} else {
					taskID := c.MapQueue.Pop()
					reply.Jobs = MapTask
					reply.FileName = c.AllTasks[taskID].FileName
					reply.TaskID = taskID
					reply.NReduce = c.NReduce

					c.TaskTrack[taskID] = &TaskStatus{taskID, MapTask, time.Now()} //Track the task
					//fmt.Printf("Assigning Map Task %d to a worker\n", taskID)
				}
			} else if args.Status == MapTask { //The worker is reporting a finished map task
				//fmt.Printf("Map Task %d Completed!\n", args.TaskID)
				taskID := args.TaskID
				delete(c.TaskTrack, taskID)
			}
		}
	} else { //Reduce phase
		if c.ReduceQue.IsEmpty() && len(c.TaskTrack) == 0 { //No more tasks to assign
			c.ReduceDone = true
		} else {
			if args.Status == 0 { // If the worker is asking for a job, then
				if c.ReduceQue.IsEmpty() {
					reply.Jobs = 0
				} else {
					taskID := c.ReduceQue.Pop()
					reply.Jobs = ReduceTask
					reply.NReduce = c.NReduce
					reply.TaskID = taskID

					c.TaskTrack[taskID] = &TaskStatus{taskID, ReduceTask, time.Now()} //Track the task
					//fmt.Printf("Assigning Reduce Task %d to a worker\n", reply.TaskID)
				}
			} else if args.Status == ReduceTask { //The worker is reporting a finished reduce task
				//fmt.Printf("Reduce Task %d Completed!\n", args.TaskID)
				taskID := args.TaskID
				delete(c.TaskTrack, taskID)
			}

		}
	}
	return nil
}

func (c *Coordinator) CheckTimeouts() {
	// Check if any tasks have timed out and reassign them
	now := time.Now()
	for _, status := range c.TaskTrack {
		if now.Sub(status.Assigned) > 10*time.Second {
			//fmt.Printf("Task %d timed out, reassigning\n", status.TaskID)
			if status.TaskType == MapTask {
				c.MapQueue.Push(status.TaskID)
			} else if status.TaskType == ReduceTask {
				c.ReduceQue.Push(status.TaskID)
			}
			delete(c.TaskTrack, status.TaskID)
		}
	}
}

func deleteIntermediateFiles() {
	dir := "intermediate"

	err := os.RemoveAll(dir)
	if err != nil {
		log.Printf("Error removing directory %s: %v", dir, err)
	}
}

// start a thread that listens for RPCs from worker.go
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

// mr-main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	ret := false

	// Your code here.
	if c.ReduceDone {
		ret = true
		deleteIntermediateFiles()
		//fmt.Println("Reduce phase completed, coordinator exiting")
	}

	return ret
}

// create a Coordinator.
// mr-main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	for TaskID, file := range files {
		c.AllTasks = append(c.AllTasks, Task{TaskID, file})
		c.MapQueue.Push(TaskID)
	}

	for i := 0; i < nReduce; i++ {
		c.ReduceQue.Push(i)
	}

	c.TaskTrack = make(map[int]*TaskStatus)
	c.NReduce = nReduce

	//fmt.Println("Coordinator initialized")
	c.server()
	return &c
}
