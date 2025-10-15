package kvsrv

import (
	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	tester "6.5840/tester1"
)

type Clerk struct {
	clnt   *tester.Clnt
	server string
}

func MakeClerk(clnt *tester.Clnt, server string) kvtest.IKVClerk {
	ck := &Clerk{clnt: clnt, server: server}
	// You may add code here.
	return ck
}

// Get fetches the current value and version for a key.  It returns
// ErrNoKey if the key does not exist. It keeps trying forever in the
// face of all other errors.
//
// You can send an RPC with code like this:
// ok := ck.clnt.Call(ck.server, "KVServer.Get", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	// You will have to modify this function.
	var args rpc.GetArgs
	var reply rpc.GetReply
	args.Key = key
	// 一直重试,直到成功
	for {
		ok := ck.clnt.Call(ck.server, "KVServer.Get", &args, &reply)
		if ok {
			return reply.Value, reply.Version, reply.Err
		}
	}
}

// Put updates key with value only if the version in the
// request matches the version of the key at the server.  If the
// versions numbers don't match, the server should return
// ErrVersion.  If Put receives an ErrVersion on its first RPC, Put
// should return ErrVersion, since the Put was definitely not
// performed at the server. If the server returns ErrVersion on a
// resend RPC, then Put must return ErrMaybe to the application, since
// its earlier RPC might have been processed by the server successfully
// but the response was lost, and the Clerk doesn't know if
// the Put was performed or not.
//
// You can send an RPC with code like this:
// ok := ck.clnt.Call(ck.server, "KVServer.Put", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Put(key, value string, version rpc.Tversion) rpc.Err {
	// You will have to modify this function.
	// 实现put-mostly-once语义

	var pargs rpc.PutArgs
	pargs.Key = key
	pargs.Value = value
	pargs.Version = version
	var preply rpc.PutReply
	first := true
	for {

		ok := ck.clnt.Call(ck.server, "KVServer.Put", &pargs, &preply)
		if ok {
			switch preply.Err {
			case rpc.OK, rpc.ErrNoKey:
				return preply.Err
			case rpc.ErrVersion:
				if first {
					// 第一次尝试就返回ErrVersion, 说明Put肯定没有成功
					return rpc.ErrVersion
				} else {
					// 不是第一次尝试就返回ErrMaybe, 说明可能成功了
					return rpc.ErrMaybe
				}
			}
		} else {
			// RPC 失败， 继续重试
		}
		first = false
	}

}
