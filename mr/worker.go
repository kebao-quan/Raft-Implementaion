package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// ByKey implements sort.Interface for []KeyValue based on
// the Key field.
type ByKey []KeyValue

// for sorting by key.
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

// mr-main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {

	// Your worker implementation here.
	for {
		reply := AskForTasks()
		if reply.Jobs == MapTask { // Map task
			fileName := reply.FileName
			fileContent := GetFileContent(fileName)
			nReduce := reply.NReduce
			kva := mapf(fileName, fileContent)

			// Partition the intermediate results across nReduce intermediate files
			encoders := make([]*json.Encoder, nReduce)
			tempFiles := make([]*os.File, nReduce)

			folderName := "intermediate"
			err := os.MkdirAll(folderName, 0755)
			if err != nil {
				log.Fatalf("Failed to create directory: %v", err)
			}

			for i := 0; i < nReduce; i++ {
				tempFile, err := os.CreateTemp(folderName, "temp-")
				if err != nil {
					log.Fatalf("Failed to create temp file: %v", err)
				}
				tempFiles[i] = tempFile
				encoders[i] = json.NewEncoder(tempFile)
			}

			for _, kv := range kva {
				r := ihash(kv.Key) % nReduce
				err := encoders[r].Encode(&kv)
				if err != nil {
					log.Fatalf("Failed to write to temp file: %v", err)
				}
			}

			// Close and rename the temp files
			for i, tempFile := range tempFiles {
				err = tempFile.Close()
				if err != nil {
					log.Fatalf("Failed to close temp file: %v", err)
				}
				targetPath := fmt.Sprintf("%s/mr-%d-%d", folderName, reply.TaskID, i)
				err = os.Rename(tempFile.Name(), targetPath)
				if err != nil {
					log.Fatalf("Failed to rename temp file: %v", err)
				}
			}

			// Tell the coordinator that the map task is done
			ReportMapTaskDone(reply.TaskID)
			//fmt.Printf("Map Task %d Completed!\n", reply.TaskID)

		} else if reply.Jobs == ReduceTask { //Reduce task
			taskID := reply.TaskID

			// Read all the intermediate files that end with the taskID
			folderName := "intermediate"
			fileNames, err := os.ReadDir(folderName)
			if err != nil {
				log.Fatalf("Failed to read directory: %v", err)
			}

			var kva []KeyValue
			for _, fileName := range fileNames {
				if fileName.Name()[len(fileName.Name())-1] == byte(taskID+'0') {
					file, err := os.Open(folderName + "/" + fileName.Name())
					if err != nil {
						log.Fatalf("Failed to open file: %v", err)
					}

					dec := json.NewDecoder(file)
					for {
						var kv KeyValue
						if err := dec.Decode(&kv); err != nil {
							break
						}
						kva = append(kva, kv)
					}
				}
			}

			// Sort the intermediate results
			sort.Sort(ByKey(kva))

			oname := fmt.Sprintf("mr-out-%d", taskID)
			ofile, _ := os.Create(oname)
			defer ofile.Close()

			//
			// call Reduce on each distinct key in intermediate[],
			// and print the result to mr-out-0.
			//
			i := 0
			for i < len(kva) {
				j := i + 1
				for j < len(kva) && kva[j].Key == kva[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, kva[k].Value)
				}
				output := reducef(kva[i].Key, values)

				// this is the correct format for each line of Reduce output.
				fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

				i = j
			}

			// Tell the coordinator that the reduce task is done
			ReportReduceTaskDone(reply.TaskID)
			//fmt.Printf("Reduce Task %d Completed!\n", reply.TaskID)

		} else if reply.Jobs == -1 { //No more tasks
			break
		} else {

		} // No tasks available, wait for 1/100 second

		time.Sleep(100 * time.Millisecond)
	}

	//fmt.Println("Worker terminated")
}

// Get the content of a file
func GetFileContent(filename string) string {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	defer file.Close()
	stat, err := file.Stat()
	if err != nil {
		log.Fatalf("cannot stat %v", filename)
	}
	content := make([]byte, stat.Size())
	_, err = file.Read(content)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}

	return string(content)
}

// Ask the coordinator for tasks
func AskForTasks() Reply {
	args := Args{}
	args.Status = 0

	reply := Reply{}

	call("Coordinator.Scheduler", &args, &reply)

	return reply
}

// Report to the coordinator that a map task is done
func ReportMapTaskDone(TaskID int) {
	args := Args{}
	args.Status = 1
	args.TaskID = TaskID

	reply := Reply{}

	call("Coordinator.Scheduler", &args, &reply)
}

// Report to the coordinator that a reduce task is done
func ReportReduceTaskDone(TaskID int) {
	args := Args{}
	args.Status = 2
	args.TaskID = TaskID

	reply := Reply{}

	call("Coordinator.Scheduler", &args, &reply)
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
