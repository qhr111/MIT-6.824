package kvsrv

import (
	"log"
	"sync"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	"6.5840/tester1"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Pair struct {
	value   string
	version rpc.Tversion
}

type KVServer struct {
	mu sync.Mutex
	kv map[string]Pair
	// Your definitions here.
}

func MakeKVServer() *KVServer {
	kv := &KVServer{
		mu: sync.Mutex{},
		kv: make(map[string]Pair),
	}
	// Your code here.
	return kv
}

// Get returns the value and version for args.Key, if args.Key
// exists. Otherwise, Get returns ErrNoKey.
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if p, ok := kv.kv[args.Key]; ok {
		reply.Value = p.value
		reply.Version = p.version
		reply.Err = rpc.OK
	} else {
		reply.Err = rpc.ErrNoKey
	}
}

// Update the value for a key if args.Version matches the version of
// the key on the server. If versions don't match, return ErrVersion.
// If the key doesn't exist, Put installs the value if the
// args.Version is 0, and returns ErrNoKey otherwise.
func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Your code here.
	//读的时候不加锁, 写的时候加锁
	kv.mu.Lock()
	//检查是否版本一致
	defer kv.mu.Unlock()

	p, ok := kv.kv[args.Key]
	switch {
	case !ok && args.Version == 0:
		// 新建 key，版本从 1 开始
		kv.kv[args.Key] = Pair{value: args.Value, version: args.Version + 1}
		reply.Err = rpc.OK
	case !ok && args.Version != 0:
		reply.Err = rpc.ErrNoKey
	case ok && args.Version != p.version:
		reply.Err = rpc.ErrVersion
	default: // ok && args.Version == p.version
		kv.kv[args.Key] = Pair{value: args.Value, version: args.Version + 1}
		reply.Err = rpc.OK
	}
}

// You can ignore Kill() for this lab
func (kv *KVServer) Kill() {
}

// You can ignore all arguments; they are for replicated KVservers
func StartKVServer(ends []*labrpc.ClientEnd, gid tester.Tgid, srv int, persister *tester.Persister) []tester.IService {
	kv := MakeKVServer()
	return []tester.IService{kv}
}
