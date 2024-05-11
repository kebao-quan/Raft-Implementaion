package kvraft

import (
	"crypto/rand"
	"cs651/labrpc"
	//"fmt"
	"math/big"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	prevLeader int

	clientId  int64
	requestId int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.prevLeader = -1

	ck.clientId = nrand()
	ck.requestId = 0

	//fmt.Printf("Clerk %d spawned!\n", ck.clientId)

	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	args := GetArgs{
		Key:       key,
		ClientId:  ck.clientId,
		RequestId: ck.requestId,
	}
	for {
		if ck.prevLeader == -1 {
			// try each known server.
			for i := 0; i < len(ck.servers); i++ {
				reply := GetReply{}
				ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
				if ok {
					switch reply.Err {
					case OK:
						ck.prevLeader = i
						//fmt.Printf("Clerk--> Get: %s, Result: %s\n", key, reply.Value)
						ck.requestId++
						return reply.Value
					case ErrNoKey:
						//fmt.Printf("Clerk--> Get: %s, Result: Empty\n", key)
						ck.requestId++
						return ""
					case ErrWrongLeader:
						ck.prevLeader = -1
					}
				}
			}
		} else {
			reply := GetReply{}
			ok := ck.servers[ck.prevLeader].Call("KVServer.Get", &args, &reply)
			if ok {
				switch reply.Err {
				case OK:
					//fmt.Printf("Clerk--> Get: %s, Result: %s\n", key, reply.Value)
					ck.requestId++
					return reply.Value
				case ErrNoKey:
					//fmt.Printf("Clerk--> Get: %s, Result: Empty\n", key)
					ck.requestId++
					return ""
				case ErrWrongLeader:
					ck.prevLeader = -1
				}
			}
		}

	}
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	args := PutAppendArgs{
		Key:       key,
		Value:     value,
		Op:        op,
		ClientId:  ck.clientId,
		RequestId: ck.requestId,
	}
	for {
		if ck.prevLeader == -1 {
			// try each known server.
			for i := 0; i < len(ck.servers); i++ {
				reply := PutAppendReply{}
				ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
				if ok {
					switch reply.Err {
					case OK:
						//fmt.Printf("Clerk--> %s: (%s, %s)\n", op, key, value)
						ck.prevLeader = i
						ck.requestId++
						return
					}
				}
			}
		} else {
			reply := PutAppendReply{}
			ok := ck.servers[ck.prevLeader].Call("KVServer.PutAppend", &args, &reply)
			if ok {
				switch reply.Err {
				case OK:
					//fmt.Printf("Clerk--> %s: (%s, %s)\n", op, key, value)
					ck.requestId++
					return
				case ErrWrongLeader:
					ck.prevLeader = -1
				}
			}
		}
	}

}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
