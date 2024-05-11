package raft

import (
	"bytes"
	"cs651/labgob"
	"cs651/labrpc"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

func DebugPrintf(format string, a ...interface{}) (n int, err error) {
	if DebugRaft {
		log.Printf(format, a...)
	}
	return
}

const (
	Follower = iota
	Candidate
	Leader

	heartbeatInterval = 100 * time.Millisecond
	DebugRaft         = false
)

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type LogEntry struct {
	Term    int
	Command interface{}
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Persistent state on all servers
	currentTerm int
	votedFor    int
	log         []LogEntry

	// Volatile state on all servers
	commitIndex int
	lastApplied int

	// Volatile state on leaders
	nextIndex  []int
	matchIndex []int

	// Channels
	applyCh             chan ApplyMsg
	applyBuffer         chan ApplyMsg
	applySignal         chan struct{}
	appendNotifyCh      chan struct{}
	heartbeatPeriodChan chan bool

	// State
	// 0: follower, 1: candidate, 2: leader
	state int

	lastIncludedIndex int
	lastIncludedTerm  int

	lastHeartbeat   time.Time
	latestIssueTime time.Time

	timeToApply map[int]time.Time
}

func (rf *Raft) GetPersister() *Persister {
	return rf.persister
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = rf.state == Leader
	rf.mu.Unlock()
	return term, isleader
}

// Checke if the server is a leader
func (rf *Raft) IsLeader() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.state == Leader
}

func (rf *Raft) GetLastLogIndex() int {
	return len(rf.log) - 1 + rf.lastIncludedIndex
}

func (rf *Raft) GetLastLogIndexLock() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return len(rf.log) - 1 + rf.lastIncludedIndex
}

func (rf *Raft) GetLastLogTerm() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.log[len(rf.log)-1].Term
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func (rf *Raft) persist() {
	// Your code here (2C).
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	// Persist the currentTerm, votedFor, and log
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)

	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var currentTerm int
	var votedFor int
	var log []LogEntry
	var lastIncludedIndex int
	var lastIncludedTerm int

	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil ||
		d.Decode(&lastIncludedIndex) != nil ||
		d.Decode(&lastIncludedTerm) != nil {
		//fmt.Println("Error reading persist")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
		rf.lastIncludedIndex = lastIncludedIndex
		rf.lastIncludedTerm = lastIncludedTerm
	}

}

func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// if rf == nil || rf.killed() {
	// 	return
	// }

	rf.mu.Lock()
	defer rf.mu.Unlock()

	DebugPrintf("Snapshot: %d received snapshot at index %d at term %d from the service\n", rf.me, index, rf.currentTerm)

	//Index already included in snapshot
	if index <= rf.lastIncludedIndex {
		log.Printf("Index already included in snapshot\n")
		return
	}

	//Index is out of bound
	if index > len(rf.log)-1+rf.lastIncludedIndex {
		log.Printf("Index is out of bound\n")
		return
	}

	//newlog := []LogEntry{{Term: rf.log[machineIndex].Term, Command: rf.log[machineIndex].Command}}
	//rf.log = append(newlog, rf.log[index-rf.lastIncludedIndex+1:]...)
	machineIndex := index - rf.lastIncludedIndex
	rf.lastIncludedIndex = index
	rf.lastIncludedTerm = rf.log[machineIndex].Term
	rf.log = rf.Truncate(machineIndex)
	rf.persist()
	rf.persister.SaveStateAndSnapshot(rf.persister.ReadRaftState(), snapshot)
}

// InstallSnapshot RPC arguments structure.
// field names must start with capital letters!
type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

// InstallSnapshot RPC reply structure.
// field names must start with capital letters!
type InstallSnapshotReply struct {
	Term int
}

// InstallSnapshot RPC handler.
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DebugPrintf("InstallSnapshot: %d received snapshot at index %d at term %d\n", rf.me, args.LastIncludedIndex, args.LastIncludedTerm)

	reply.Term = rf.currentTerm

	//1. Reply immediately if term < currentTerm
	if args.Term < rf.currentTerm {
		return
	}

	//If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower (§5.1)
	if args.Term > rf.currentTerm {
		rf.state = Follower
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.persist()
	}

	rf.lastHeartbeat = time.Now()

	if args.LastIncludedIndex <= rf.lastIncludedIndex {
		//fmt.Println("InstallSnapshot: ", rf.me, " received snapshot at index ", args.LastIncludedIndex, " at term ", args.LastIncludedTerm, " but already has snapshot at index ", rf.lastIncludedIndex, " at term ", rf.lastIncludedTerm)
		//fmt.Println(rf.log)
		return
	}

	//5. Save snapshot file, discard any existing or partial snapshot with a smaller index
	lastIndex := len(rf.log) - 1 + rf.lastIncludedIndex
	lastIncludedIndexTemp := rf.lastIncludedIndex
	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm

	//6. If existing log entry has same index and term as snapshot’s
	//last included entry, retain log entries following it and reply
	if args.LastIncludedIndex < lastIndex {
		if rf.log[args.LastIncludedIndex-lastIncludedIndexTemp].Term == args.LastIncludedTerm {
			rf.log = rf.Truncate(args.LastIncludedIndex - lastIncludedIndexTemp)
			rf.persist()
			rf.persister.SaveStateAndSnapshot(rf.persister.ReadRaftState(), args.Data)
			//apply
			rf.applyBuffer <- ApplyMsg{
				SnapshotValid: true,
				Snapshot:      args.Data,
				SnapshotTerm:  rf.lastIncludedTerm,
				SnapshotIndex: rf.lastIncludedIndex,
			}
			//fmt.Println("Applying snapshot at index ", rf.lastIncludedIndex, " at term ", rf.lastIncludedTerm, " by ", rf.me)
			rf.lastApplied = args.LastIncludedIndex
			return
		}
	}

	//7. Discard the entire log
	rf.log = []LogEntry{{Term: args.LastIncludedTerm, Command: nil}}
	rf.lastApplied = args.LastIncludedIndex
	rf.persist()
	rf.persister.SaveStateAndSnapshot(rf.persister.ReadRaftState(), args.Data)

	//apply
	rf.applyBuffer <- ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  rf.lastIncludedTerm,
		SnapshotIndex: rf.lastIncludedIndex,
	}

	//fmt.Println("Applying snapshot at index ", rf.lastIncludedIndex, " at term ", rf.lastIncludedTerm, " by ", rf.me)

}

func (rf *Raft) Truncate(index int) []LogEntry {
	if index < 0 || index > len(rf.log) {
		// If the index is out of bounds, return the original slice
		return rf.log
	}
	// Use append to keep the first element and then append elements after the given index
	return rf.log[index:]
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	//Reply false if term < currentTerm (§5.1)
	if args.Term < rf.currentTerm {
		return
	}

	//If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower (§5.1)
	if args.Term > rf.currentTerm {
		rf.state = Follower
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.persist()
	}

	//If votedFor is null or candidateId, and candidate’s log is at least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		if args.LastLogTerm > rf.log[len(rf.log)-1].Term || (args.LastLogTerm == rf.log[len(rf.log)-1].Term && args.LastLogIndex >= (len(rf.log)-1)+rf.lastIncludedIndex) {
			rf.votedFor = args.CandidateId
			rf.persist()
			reply.VoteGranted = true
			rf.lastHeartbeat = time.Now()
		}
	}
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) startElection() {
	votes := 1
	majority := len(rf.peers)/2 + 1
	finished := 0
	totalPeers := len(rf.peers)
	timeOut := randTimeout()
	start := time.Now()

	rf.mu.Lock()
	rf.state = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.persist()

	lastIndex := len(rf.log) - 1 + rf.lastIncludedIndex
	lastTerm := rf.log[len(rf.log)-1].Term

	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: lastIndex,
		LastLogTerm:  lastTerm,
	}
	rf.mu.Unlock()

	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}

		go func(i int) {
			reply := RequestVoteReply{}
			ok := rf.sendRequestVote(i, &args, &reply)
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if rf.state != Candidate || rf.currentTerm != args.Term {
				return
			}
			if ok {
				finished++
				if reply.Term > rf.currentTerm {
					rf.state = Follower
					rf.currentTerm = reply.Term
					rf.votedFor = -1
					rf.persist()
					return
				}
				if reply.VoteGranted {
					votes++
				}
			} else {
				finished++
			}
		}(peer)
	}

	for {
		rf.mu.Lock()
		if votes >= majority {
			if rf.state == Candidate {
				//log.Println(rf.me, " becomes leader at term: ", rf.currentTerm)
				rf.state = Leader
				for i := 0; i < len(rf.peers); i++ {
					rf.nextIndex[i] = len(rf.log) + rf.lastIncludedIndex
					DebugPrintf("rf.nextIndex[%d] = %d\n", i, rf.nextIndex[i])
					rf.matchIndex[i] = 0
				}
				go rf.broadcastAppenEntries()
				go rf.heartbeatPeriodTick()
			}
			rf.mu.Unlock()
			break
		}

		if finished >= totalPeers-1 || rf.state != Candidate || time.Since(start) >= timeOut {
			rf.state = Follower
			rf.mu.Unlock()
			break
		}

		if rf.killed() {
			rf.mu.Unlock()
			return
		}

		rf.mu.Unlock()
		time.Sleep(30 * time.Millisecond)
	}
}

func (rf *Raft) tryCommit() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//Check match index and find majority, if majority commit
	quorum := len(rf.peers)/2 + 1
	for i := rf.GetLastLogIndex(); i > rf.commitIndex; i-- {
		count := 1
		for j := 0; j < len(rf.peers); j++ {
			if j != rf.me && rf.matchIndex[j] >= i {
				count++
			}
		}
		if count >= quorum {
			//DebugPrintf("index %d, Time to reach quorum: %d\n", i, time.Since(rf.timeToApply[i]).Milliseconds())
			rf.commitIndex = i
			rf.notifyApply()
			rf.notifyAppend()
			break
		} else {
			DebugPrintf("did not reach quorum for index %d. Count: %d, quorum: %d\n", i, count, quorum)
			DebugPrintf("matchindex: %v\n", rf.matchIndex)
		}
	}
}

func (rf *Raft) broadcastAppenEntries() {
	var wg sync.WaitGroup
	for !rf.killed() {
		//DebugPrintf("Leader %d: broadcastAppenEntries\n", rf.me)
		select {
		case <-rf.appendNotifyCh:
			//Got notify from applyCh, check if there is any new entry to send
		case <-rf.heartbeatPeriodChan:
			// rf.mu.Lock()
			// broadcasttime, ok := rf.timeToApply[rf.GetLastLogIndex()]
			// if ok {
			// 	DebugPrintf("Leader %d: last log index: %d, time at Heartbeat: %d\n", rf.me, rf.GetLastLogIndex(), time.Since(broadcasttime).Milliseconds())
			// } else {
			// 	DebugPrintf("Leader %d: last log index: %d, time at Heartbeat, not ok: %d\n", rf.me, rf.GetLastLogIndex(), 0)
			// }
			// rf.mu.Unlock()
			rf.mu.Lock()
			rf.latestIssueTime = time.Now()
			rf.mu.Unlock()
		}
		//DebugPrintf("Leader %d: broadcastAppenEntries, after select\n", rf.me)

		if _, isLeader := rf.GetState(); !isLeader {
			return
		}

		for peer := range rf.peers {
			if peer == rf.me {
				continue
			}

			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				rf.mu.Lock()
				//If nextIndex > lastIncludedIndex, send appendEntries
				if rf.nextIndex[i] > rf.lastIncludedIndex {
					currentTerm := rf.currentTerm
					LeaderId := rf.me
					nextIndex := rf.nextIndex[i]
					PrevLogIndex := rf.nextIndex[i] - 1
					PrevLogTerm := rf.log[rf.nextIndex[i]-1-rf.lastIncludedIndex].Term
					LeaderCommit := rf.commitIndex
					lastIncludedIndex := rf.lastIncludedIndex
					entries := make([]LogEntry, len(rf.log[nextIndex-rf.lastIncludedIndex:]))
					copy(entries, rf.log[nextIndex-rf.lastIncludedIndex:])
					rf.mu.Unlock()

					args := AppendEntriesArgs{
						Term:         currentTerm,
						LeaderId:     LeaderId,
						PrevLogIndex: PrevLogIndex,
						PrevLogTerm:  PrevLogTerm,
						Entries:      entries,
						LeaderCommit: LeaderCommit,
					}
					reply := AppendEntriesReply{}
					ok := rf.sendAppendEntries(i, &args, &reply)

					if !ok {
						return
					}
					// rf.mu.Lock()
					// broadcasttime, ok_ := rf.timeToApply[rf.GetLastLogIndex()]
					// if ok_ {
					// 	DebugPrintf("Leader %d sends apendEntries with index %d to %d, time at sendAppendEntries: %d\n", rf.me, rf.GetLastLogIndex(), i, time.Since(broadcasttime).Milliseconds())
					// }
					// rf.mu.Unlock()
					rf.mu.Lock()
					defer rf.mu.Unlock()

					if rf.state != Leader || rf.currentTerm != currentTerm {
						return
					}

					//A later AppendEntries RPC call responding earlier then the current one
					if rf.nextIndex[i] != nextIndex || lastIncludedIndex != rf.lastIncludedIndex {
						DebugPrintf("Leader %d: nextIndex[%d] changed from %d to %d\n", rf.me, i, nextIndex, rf.nextIndex[i])
						//log.Panic("nextIndex changed")
						return
					}

					//If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower (§5.1)
					if reply.Term > rf.currentTerm {
						rf.state = Follower
						rf.currentTerm = reply.Term
						rf.votedFor = -1
						rf.persist()
						return
					}
					if reply.Success {
						rf.nextIndex[i] += len(entries)
						rf.matchIndex[i] = rf.nextIndex[i] - 1
						//DebugPrintf("Leader %d, request to %d, success, nextIndex[%d] = %d\n", rf.me, i, i, rf.nextIndex[i])
					} else {
						if reply.ConflictTerm != -1 {
							if reply.ConflictFirstIndex < rf.lastIncludedIndex {
								rf.nextIndex[i] = reply.ConflictFirstIndex
								DebugPrintf("rf.nextIndex[%d] = %d\n", i, rf.nextIndex[i])
							} else if rf.log[reply.ConflictFirstIndex-rf.lastIncludedIndex].Term != reply.ConflictTerm {
								rf.nextIndex[i] = reply.ConflictFirstIndex
								DebugPrintf("rf.nextIndex[%d] = %d\n", i, rf.nextIndex[i])
							} else {
								for j := reply.ConflictFirstIndex; j < len(rf.log); j++ {
									if rf.log[j-rf.lastIncludedIndex].Term != reply.ConflictTerm {
										rf.nextIndex[i] = j - 1
										DebugPrintf("rf.nextIndex[%d] = %d\n", i, rf.nextIndex[i])
										break
									}
								}
							}
						}
						if reply.XLen != -1 {
							rf.nextIndex[i] = reply.XLen + 1
							DebugPrintf("rf.nextIndex[%d] = %d\n", i, rf.nextIndex[i])
						}
						rf.notifyAppend()
						DebugPrintf("Leader %d, request to %d, fail, nextIndex[%d] = %d\n", rf.me, i, i, rf.nextIndex[i])
					}
				} else {
					args := InstallSnapshotArgs{
						Term:              rf.currentTerm,
						LeaderId:          rf.me,
						LastIncludedIndex: rf.lastIncludedIndex,
						LastIncludedTerm:  rf.lastIncludedTerm,
						Data:              rf.persister.ReadSnapshot(),
					}
					rf.mu.Unlock()

					reply := InstallSnapshotReply{}

					//fmt.Println("Leader ", rf.me, " sending snapshot to ", i)
					ok := rf.sendInstallSnapshot(i, &args, &reply)
					if !ok {
						return
					}

					rf.mu.Lock()
					defer rf.mu.Unlock()

					if rf.state != Leader || rf.currentTerm != args.Term {
						return
					}

					if rf.currentTerm < reply.Term {
						rf.state = Follower
						rf.currentTerm = reply.Term
						rf.votedFor = -1
						rf.persist()
						return
					}

					rf.nextIndex[i] = rf.lastIncludedIndex + 1
					DebugPrintf("rf.nextIndex[%d] = %d\n", i, rf.nextIndex[i])
				}
			}(peer)
		}
		wg.Wait()
		// rf.mu.Lock()
		// broadcasttime, ok := rf.timeToApply[rf.GetLastLogIndex()]
		// if ok {
		// 	DebugPrintf("Leader %d: Index: %d, time at all response received: %d, Match Index: %v\n", rf.me, rf.GetLastLogIndex(), time.Since(broadcasttime).Milliseconds(), rf.matchIndex)
		// }
		// rf.mu.Unlock()
		rf.tryCommit()
	}
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term               int
	Success            bool
	ConflictFirstIndex int
	ConflictTerm       int
	XLen               int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.Success = false
	reply.ConflictFirstIndex = -1
	reply.ConflictTerm = -1
	reply.XLen = -1

	//Reply false if term < currentTerm (§5.1)
	if args.Term < rf.currentTerm {
		return
	}

	//If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower (§5.1)
	if args.Term > rf.currentTerm {
		rf.state = Follower
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.persist()
	}

	rf.lastHeartbeat = time.Now()

	if args.PrevLogIndex < rf.lastIncludedIndex {
		if len(args.Entries) > (rf.lastIncludedIndex - args.PrevLogIndex) {
			rf.log = append(rf.log[:1], args.Entries[rf.lastIncludedIndex-args.PrevLogIndex:]...)
			rf.log[0].Command = nil
			rf.persist()
		}
		reply.Success = true
		return
	}

	//Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
	if args.PrevLogIndex > rf.GetLastLogIndex() {
		reply.XLen = rf.GetLastLogIndex()
		//reply.ConflictTerm = -1
		//reply.ConflictFirstIndex = rf.GetLastLogIndex() + 1
		return
	}

	//If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it (§5.3)
	if rf.log[args.PrevLogIndex-rf.lastIncludedIndex].Term != args.PrevLogTerm {
		reply.ConflictTerm = rf.log[args.PrevLogIndex-rf.lastIncludedIndex].Term
		reply.ConflictFirstIndex = args.PrevLogIndex
		for i := args.PrevLogIndex - 1 - rf.lastIncludedIndex; i > 0; i-- {
			if rf.log[i].Term != reply.ConflictTerm {
				//log.Printf("rf.lastIncludedIndex: %d, args.PrevLogIndex: %d, reply.ConflictFirstIndex: %d, reply.ConflictTerm: %d\n", rf.lastIncludedIndex, args.PrevLogIndex, reply.ConflictFirstIndex, reply.ConflictTerm)
				break
			} else {
				reply.ConflictFirstIndex--

			}
		}
		DebugPrintf("ConflictFirstIndex: %d, ConflictTerm: %d\n", reply.ConflictFirstIndex, reply.ConflictTerm)
		return
	}

	//Append any new entries not already in the log
	rf.log = rf.log[:args.PrevLogIndex+1-rf.lastIncludedIndex]
	rf.persist()
	if len(args.Entries) > 0 {
		rf.log = append(rf.log, args.Entries...)
		rf.persist()
	}
	//DebugPrintf("Success, ConflictFirstIndex: %d, ConflictTerm: %d\n", reply.ConflictFirstIndex, reply.ConflictTerm)
	reply.Success = true

	//If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(rf.GetLastLogIndex(), args.LeaderCommit)
		rf.notifyApply()
	}
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	if !rf.IsLeader() {
		return index, term, false
	}

	rf.mu.Lock()
	term = rf.currentTerm
	index = len(rf.log) + rf.lastIncludedIndex
	rf.log = append(rf.log, LogEntry{Term: term, Command: command})
	rf.persist()
	rf.timeToApply[index] = time.Now()
	rf.mu.Unlock()

	rf.notifyAppend()

	// rf.mu.Lock()
	// DebugPrintf("Leader %d received command %d at index %d+%d=%d at term %d", rf.me, command, len(rf.log)-1, rf.lastIncludedIndex, index, term)
	// rf.mu.Unlock()

	return index, term, isLeader
}

func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func randTimeout() time.Duration {
	return time.Duration(rand.Intn(151)+300) * time.Millisecond
}

// The ticker go routine starts a new election if this peer hasn't received heartsbeats recently.
func (rf *Raft) ticker() {
	for !rf.killed() {
		electionTimeout := randTimeout()

		time.Sleep(electionTimeout)

		rf.mu.Lock()
		if rf.state == Follower && time.Since(rf.lastHeartbeat) >= electionTimeout {
			rf.mu.Unlock()
			rf.startElection()
		} else {
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) heartbeatPeriodTick() {
	for !rf.killed() && rf.IsLeader() {
		rf.mu.Lock()
		elapseTime := time.Since(rf.latestIssueTime)
		if elapseTime >= heartbeatInterval {
			select {
			case rf.heartbeatPeriodChan <- true:
				// Notification sent
			default:
				// Channel already has a notification, do not block
			}
		}
		rf.mu.Unlock()
		time.Sleep(time.Millisecond * 10)
	}
}

func (rf *Raft) logApplier() {
	rf.mu.Lock()
	//Upon restart, load snapshot
	if rf.lastIncludedIndex > 0 {
		rf.applyBuffer <- ApplyMsg{
			SnapshotValid: true,
			Snapshot:      rf.persister.ReadSnapshot(),
			SnapshotTerm:  rf.lastIncludedTerm,
			SnapshotIndex: rf.lastIncludedIndex,
		}
		rf.lastApplied = rf.lastIncludedIndex
	}
	rf.mu.Unlock()

	for !rf.killed() {
		<-rf.applySignal
		rf.mu.Lock()
		if rf.commitIndex > rf.lastApplied {
			for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
				// This happens when Msg and not yet been apply has been dropped becuase of snapshot.
				// In this case, we should ignore it since it has already been applied by the snapshot.
				if i-rf.lastIncludedIndex <= 0 {
					//log.Printf("peer: %d, i: %d, rf.lastIncludedIndex: %d, commitIndex: %d, isleader: %v\n", rf.me, i, rf.lastIncludedIndex, rf.commitIndex, rf.state == Leader)
					continue
				}
				rf.applyBuffer <- ApplyMsg{
					CommandValid: true,
					Command:      rf.log[i-rf.lastIncludedIndex].Command,
					CommandIndex: i,
				}
				DebugPrintf("Committing Index: %d, Term: %d, by %d\n", i, rf.log[i-rf.lastIncludedIndex].Term, rf.me)
			}
			rf.lastApplied = rf.commitIndex
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) apply() {
	for !rf.killed() {
		msg := <-rf.applyBuffer
		rf.applyCh <- msg

		rf.mu.Lock()
		if rf.state == Leader && true {
			//log.Printf("Index: %d, Command: %v, time to apply: %d\n", msg.CommandIndex, msg.Command, time.Since(rf.timeToApply[msg.CommandIndex]).Milliseconds())
		}

		delete(rf.timeToApply, msg.CommandIndex)
		rf.mu.Unlock()
	}
}

func (rf *Raft) notifyAppend() {
	select {
	case rf.appendNotifyCh <- struct{}{}:
		// Notification sent
	default:
		// Channel already has a notification, do not block
	}
}

func (rf *Raft) notifyApply() {
	select {
	case rf.applySignal <- struct{}{}:
		// Notification sent
	default:
		// Channel already has a notification, do not block
	}
}

// func (rf *Raft) test() {
// 	for !rf.killed() {
// 		rf.mu.Lock()
// 		type logConcise struct {
// 			Index int
// 			Term  int
// 		}
// 		var logs []logConcise
// 		for i := 0; i < len(rf.log); i++ {
// 			logs = append(logs, logConcise{Index: i + rf.lastIncludedIndex, Term: rf.log[i].Term})
// 		}

// 		isLeader := rf.state == Leader
// 		log.Println(rf.me, isLeader, " commitIndex: ", rf.commitIndex, "lastApplied: ", rf.lastApplied, " log: ", logs)
// 		rf.mu.Unlock()
// 		time.Sleep(200 * time.Millisecond)
// 	}
// }

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	DebugPrintf("Make: %d spawned!\n", me)
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = []LogEntry{{Term: 0, Command: 0}}
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.applyCh = applyCh
	rf.applyBuffer = make(chan ApplyMsg, 100000)
	rf.applySignal = make(chan struct{}, 1)
	rf.appendNotifyCh = make(chan struct{}, 1)
	rf.heartbeatPeriodChan = make(chan bool, 1)
	rf.state = Follower

	rf.lastIncludedIndex = 0
	rf.lastIncludedTerm = 0
	rf.latestIssueTime = time.Now()

	rf.timeToApply = make(map[int]time.Time)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	//
	go rf.logApplier()
	go rf.apply()

	//go rf.test()

	return rf
}
