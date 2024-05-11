package kvraft

import (
	"bytes"
	"cs651/labgob"
	"cs651/labrpc"
	"cs651/raft"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.

	// OpType: "Get", "Put", "Append"
	OpType string
	// Key: key to get, put, or append
	Key string
	// Value: value to put or append
	Value string
	// ClientId: client id
	ClientId int64
	// RequestId: request id
	RequestId int64
}

type LatestReply struct {
	RequestId int64
	Reply     GetReply
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	keyValueMap  map[string]string
	opNotifyMap  map[Op]chan GetReply
	duplicateMap map[int64]LatestReply //mapping of Client ID to (max seen Seq Number & corresponding respond value)
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	if kv.killed() {
		reply.Err = ErrWrongLeader
		return
	}

	//check duplicate
	kv.mu.Lock()
	latest, ok := kv.duplicateMap[args.ClientId]
	if ok && args.RequestId <= latest.RequestId {
		kv.mu.Unlock()
		reply.Err = latest.Reply.Err
		reply.Value = latest.Reply.Value
		return
	}
	kv.mu.Unlock()
	op := Op{OpType: "Get", Key: args.Key, ClientId: args.ClientId, RequestId: args.RequestId}
	_, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	notifyChan := make(chan GetReply)

	kv.mu.Lock()
	kv.opNotifyMap[op] = notifyChan
	kv.mu.Unlock()

	select {
	case <-time.After(300 * time.Millisecond):
		//fmt.Println("KvServer: Get timeout")
		reply.Err = ErrWrongLeader
	case res := <-notifyChan:
		reply.Err = res.Err
		reply.Value = res.Value
	}

	kv.mu.Lock()
	delete(kv.opNotifyMap, op)
	kv.mu.Unlock()
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	if kv.killed() {
		reply.Err = ErrWrongLeader
		return
	}

	//check duplicate
	kv.mu.Lock()

	latest, ok := kv.duplicateMap[args.ClientId]
	if ok && args.RequestId <= latest.RequestId {
		kv.mu.Unlock()
		//fmt.Printf("KvServer: Leader %d PutAppend: (%s, %s) duplicate\n", kv.me, args.Key, args.Value)
		reply.Err = latest.Reply.Err
		return
	}
	kv.mu.Unlock()

	op := Op{
		OpType:    args.Op,
		Key:       args.Key,
		Value:     args.Value,
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
	}

	_, _, isLeader := kv.rf.Start(op)
	//startTime := time.Now()

	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	//fmt.Println(len(kv.duplicateMap))
	//fmt.Printf("KvServer: Leader %d recerves PutAppend: (%s, %s) \n", kv.me, args.Key, args.Value)

	notifyChan := make(chan GetReply)

	kv.mu.Lock()
	kv.opNotifyMap[op] = notifyChan

	kv.mu.Unlock()

	select {
	case <-time.After(300 * time.Millisecond):
		//fmt.Printf("KvServer: Leader %d PutAppend: (%s, %s) timeout \n", kv.me, args.Key, args.Value)
		reply.Err = ErrWrongLeader
	case res := <-notifyChan:
		reply.Err = res.Err
		//fmt.Printf("KvServer: Leader %d PutAppend: (%s, %s) Success, time: %d \n", kv.me, args.Key, args.Value, time.Since(startTime).Milliseconds())
	}
	kv.mu.Lock()
	delete(kv.opNotifyMap, op)
	kv.mu.Unlock()
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me

	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.opNotifyMap = make(map[Op]chan GetReply)
	kv.duplicateMap = make(map[int64]LatestReply)

	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.keyValueMap = make(map[string]string)

	// Read snapshot
	data := kv.rf.GetPersister().ReadSnapshot()
	if data != nil && len(data) > 0 {
		kv.readSnapshot(data)
	}

	go kv.Run()

	return kv
}

func (kv *KVServer) readSnapshot(data []byte) {
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var keyValueMap map[string]string
	var duplicateMap map[int64]LatestReply

	if d.Decode(&keyValueMap) != nil ||
		d.Decode(&duplicateMap) != nil {
		DPrintf("KvServer: readSnapshot error")
	} else {
		kv.keyValueMap = keyValueMap
		kv.duplicateMap = duplicateMap
	}
}

func (kv *KVServer) encodeSnapshot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.keyValueMap)
	e.Encode(kv.duplicateMap)
	data := w.Bytes()
	return data
}

func (kv *KVServer) Run() {
	for !kv.killed() {
		msg := <-kv.applyCh
		// if msg.UseSnapshot {
		// 	kv.mu.Lock()
		// 	kv.readSnapshot(msg.Snapshot)
		// 	kv.mu.Unlock()
		// 	continue
		// }
		// DPrintf("%d", kv.maxraftstate)
		if msg.CommandValid == false && msg.SnapshotValid == true {
			kv.mu.Lock()
			kv.readSnapshot(msg.Snapshot)
			kv.mu.Unlock()
			continue
		}

		op := msg.Command.(Op)

		//DPrintf("KvServer: applying %s: (%s, %s) \n", op.OpType, op.Key, op.Value)
		kv.mu.Lock()
		notifyChan := kv.opNotifyMap[op]
		latest, latestExist := kv.duplicateMap[op.ClientId]
		kv.mu.Unlock()
		reply := GetReply{}
		switch op.OpType {
		case "Get":
			if latestExist && op.RequestId <= latest.RequestId {
				reply = latest.Reply
			} else {
				value, ok := kv.keyValueMap[op.Key]
				if ok {
					reply.Err = OK
					reply.Value = value
				} else {
					reply.Err = ErrNoKey
				}
				kv.mu.Lock()
				kv.duplicateMap[op.ClientId] = LatestReply{RequestId: op.RequestId, Reply: reply}
				kv.mu.Unlock()
			}

			if notifyChan != nil { //if the request is still waiting for response, send the response
				notifyChan <- reply
			}
		case "Put":
			reply.Err = OK

			//update key value map and duplicate map
			//If duplicates arrive before original applies? Replicated command appears twice on log, ignore 2nd one (or later ones)
			if !latestExist || op.RequestId > latest.RequestId {
				kv.keyValueMap[op.Key] = op.Value

				kv.mu.Lock()
				kv.duplicateMap[op.ClientId] = LatestReply{RequestId: op.RequestId, Reply: reply}
				kv.mu.Unlock()
			}

			//notify waiting request=
			if notifyChan != nil {
				notifyChan <- reply
			}
		case "Append":
			reply.Err = OK

			//update key value map and duplicate map
			if !latestExist || op.RequestId > latest.RequestId {
				kv.keyValueMap[op.Key] += op.Value

				kv.mu.Lock()
				kv.duplicateMap[op.ClientId] = LatestReply{RequestId: op.RequestId, Reply: reply}
				kv.mu.Unlock()
			} else {
				reply.Err = latest.Reply.Err
			}

			if notifyChan != nil {
				notifyChan <- reply
			}
		}

		//Check if the size of raft state exceeds maxraftstate, if so, snapshot
		if len(kv.rf.GetPersister().ReadRaftState()) >= kv.maxraftstate && kv.maxraftstate != -1 {
			DPrintf("KvServer: snapshotting index %d\n", msg.CommandIndex)
			data := kv.encodeSnapshot()
			kv.rf.Snapshot(msg.CommandIndex, data)
		}
	}
}
