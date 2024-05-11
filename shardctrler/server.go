package shardctrler

import (
	"cs651/labgob"
	"cs651/labrpc"
	"cs651/raft"
	"fmt"
	"hash/fnv"
	"log"
	"sync"
	"time"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	opNotifyMap map[string]chan Config

	configs []Config // indexed by config num
}

type Op struct {
	OpType string

	// JoinArgs
	Servers map[int][]string

	// LeaveArgs
	GIDs []int

	// MoveArgs
	Shard int
	GID   int

	// QueryArgs
	Num int

	// ClientId
	//ClientId int64
	// RequestId
	//RequestId int64
}

// hashOp creates a hash of an Op object.
func hashOp(op Op) string {
	h := fnv.New32a()
	h.Write([]byte(fmt.Sprintf("%s", op.OpType)))
	h.Write([]byte(fmt.Sprintf("%v", op.Servers)))
	h.Write([]byte(fmt.Sprintf("%v", op.GIDs)))
	h.Write([]byte(fmt.Sprintf("%d", op.Shard)))
	h.Write([]byte(fmt.Sprintf("%d", op.GID)))
	h.Write([]byte(fmt.Sprintf("%d", op.Num)))
	return fmt.Sprintf("%x", h.Sum32())
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	op := Op{OpType: "Join", Servers: args.Servers}
	_, _, isLeader := sc.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		return
	}

	reply.WrongLeader = false

	//wait for the result
	notifyChan := make(chan Config)

	//Use hash of op as key because op is not comparable
	sc.mu.Lock()
	key := hashOp(op)
	sc.opNotifyMap[key] = notifyChan
	sc.mu.Unlock()

	select {
	case <-time.After(300 * time.Millisecond):
		reply.WrongLeader = true
	case <-notifyChan:
		reply.WrongLeader = false
	}

	sc.mu.Lock()
	close(notifyChan)
	delete(sc.opNotifyMap, key)
	sc.mu.Unlock()
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	op := Op{OpType: "Leave", GIDs: args.GIDs}
	_, _, isLeader := sc.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		return
	}

	reply.WrongLeader = false

	//wait for the result
	notifyChan := make(chan Config)

	//Use hash of op as key because op is not comparable
	sc.mu.Lock()
	key := hashOp(op)
	sc.opNotifyMap[key] = notifyChan
	sc.mu.Unlock()

	select {
	case <-time.After(300 * time.Millisecond):
		reply.WrongLeader = true
	case <-notifyChan:
		reply.WrongLeader = false
	}

	sc.mu.Lock()
	close(notifyChan)
	delete(sc.opNotifyMap, key)
	sc.mu.Unlock()
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	op := Op{OpType: "Move", Shard: args.Shard, GID: args.GID}
	_, _, isLeader := sc.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		return
	}

	reply.WrongLeader = false

	//wait for the result
	notifyChan := make(chan Config)

	//Use hash of op as key because op is not comparable
	sc.mu.Lock()
	key := hashOp(op)
	sc.opNotifyMap[key] = notifyChan
	sc.mu.Unlock()

	select {
	case <-time.After(300 * time.Millisecond):
		reply.WrongLeader = true
	case <-notifyChan:
		reply.WrongLeader = false
	}

	sc.mu.Lock()
	close(notifyChan)
	delete(sc.opNotifyMap, key)
	sc.mu.Unlock()
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	op := Op{OpType: "Query", Num: args.Num}
	_, _, isLeader := sc.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		return
	}

	reply.WrongLeader = false

	//wait for the result
	notifyChan := make(chan Config)

	//Use hash of op as key because op is not comparable
	sc.mu.Lock()
	key := hashOp(op)
	sc.opNotifyMap[key] = notifyChan
	sc.mu.Unlock()

	select {
	case <-time.After(300 * time.Millisecond):
		reply.WrongLeader = true
	case config := <-notifyChan:
		DPrintf("Return Query: %v\n", config)
		reply.Config = config
		reply.WrongLeader = false
	}

	sc.mu.Lock()
	close(notifyChan)
	delete(sc.opNotifyMap, key)
	sc.mu.Unlock()
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	//The very first configuration should be numbered zero. It should contain no groups, and all shards should be assigned to GID zero (an invalid GID). The next configuration (created in response to a Join RPC) should be numbered 1, &c. There will usually be significantly more shards than groups (i.e., each group will serve more than one shard), in order that load can be shifted at a fairly fine granularity.
	sc.configs[0].Num = 0
	sc.configs[0].Shards = [NShards]int{}
	for i := 0; i < NShards; i++ {
		sc.configs[0].Shards[i] = 0
	}

	sc.opNotifyMap = make(map[string]chan Config)

	go sc.Run()

	return sc
}

func (sc *ShardCtrler) Run() {
	for {
		msg := <-sc.applyCh
		if !msg.CommandValid {
			continue
		}
		op := msg.Command.(Op)
		config := Config{}
		switch op.OpType {
		case "Join":
			sc.join(op)
		case "Leave":
			sc.leave(op)
		case "Move":
			sc.move(op)
		case "Query":
			config = sc.query(op)
		}
		sc.mu.Lock()
		notifyChan, ok := sc.opNotifyMap[hashOp(op)]
		if ok {
			notifyChan <- config
		}
		sc.mu.Unlock()
	}
}

func (sc *ShardCtrler) copyLastConfig() Config {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	newConfig := Config{}
	newConfig.Num = sc.configs[len(sc.configs)-1].Num
	newConfig.Shards = sc.configs[len(sc.configs)-1].Shards
	newConfig.Groups = make(map[int][]string)
	for k, v := range sc.configs[len(sc.configs)-1].Groups {
		newConfig.Groups[k] = v
	}
	return newConfig
}

func (sc *ShardCtrler) rebalance(config *Config) {
	Shard := config.Shards
	for i, v := range Shard {
		_, ok := config.Groups[v]
		if !ok {
			//The group is deleted
			//Set shard to invalid group(0)
			Shard[i] = 0
		}
	}

	//count the number of shards for each group
	groupShardCount := make(map[int]int)
	unassignedShard := make([]int, 0)

	//initialize the all the group shard count to 0
	for k := range config.Groups {
		groupShardCount[k] = 0
	}

	//count the number of shards for each group
	for i, v := range Shard {
		if v == 0 {
			unassignedShard = append(unassignedShard, i)
			continue
		}
		groupShardCount[v]++
	}

	//find the group with minimum shards.
	for len(unassignedShard) > 0 {
		minShardCount := NShards
		minGid := 0
		for k := range groupShardCount {
			if groupShardCount[k] < minShardCount {
				minShardCount = groupShardCount[k]
				minGid = k
			} else if groupShardCount[k] == minShardCount && k < minGid {
				minGid = k
			}
		}

		//assign the unassigned shard to the group with minimum shards
		groupShardCount[minGid]++
		Shard[unassignedShard[0]] = minGid
		unassignedShard = unassignedShard[1:]
	}

	//rebalance the shards
	//While there exists a group g with more than the average number of shards and a group h with fewer than the average number of shards, move one shard from g to h.
	for {
		maxShardCount := 0
		maxGid := 0
		minShardCount := NShards
		minGid := 0
		for k, v := range groupShardCount {
			if v > maxShardCount {
				maxShardCount = v
				maxGid = k
			} else if v == maxShardCount && k < maxGid {
				maxGid = k
			}

			if v < minShardCount {
				minShardCount = v
				minGid = k
			} else if v == minShardCount && k < minGid {
				minGid = k
			}
		}

		if maxShardCount-minShardCount <= 1 {
			break
		}

		//move one shard from g to h
		for i, v := range Shard {
			if v == maxGid {
				Shard[i] = minGid
				groupShardCount[maxGid]--
				groupShardCount[minGid]++
				break
			}
		}
	}

	config.Shards = Shard

}

func (sc *ShardCtrler) appendConfig(config *Config) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	config.Num = sc.configs[len(sc.configs)-1].Num + 1
	sc.configs = append(sc.configs, *config)
}

func (sc *ShardCtrler) join(op Op) {
	if sc.rf.IsLeader() {
		DPrintf("Join: %v\n", op.Servers)
	}

	newConfig := sc.copyLastConfig()
	for k, v := range op.Servers {
		newConfig.Groups[k] = v
	}
	sc.rebalance(&newConfig)
	sc.appendConfig(&newConfig)
}

func (sc *ShardCtrler) leave(op Op) {
	if sc.rf.IsLeader() {
		DPrintf("Leave: %v\n", op.GIDs)
	}

	newConfig := sc.copyLastConfig()
	for _, v := range op.GIDs {
		delete(newConfig.Groups, v)
	}
	sc.rebalance(&newConfig)
	sc.appendConfig(&newConfig)
}

func (sc *ShardCtrler) move(op Op) {
	if sc.rf.IsLeader() {
		DPrintf("Move: %v\n", op)
	}
	newConfig := sc.copyLastConfig()
	newConfig.Shards[op.Shard] = op.GID
	sc.appendConfig(&newConfig)
}

func (sc *ShardCtrler) query(op Op) Config {
	if sc.rf.IsLeader() {
		DPrintf("Query: %v\n", op)
	}

	var config Config

	if op.Num == -1 || op.Num >= len(sc.configs) {
		config = sc.configs[len(sc.configs)-1]
	} else {
		config = sc.configs[op.Num]
	}

	DPrintf("Query retrun from %d: %v\n", sc.me, config)

	return config
}
