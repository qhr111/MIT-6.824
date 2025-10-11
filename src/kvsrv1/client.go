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

	var pargs rpc.PutArgs
	pargs.Key = key
	pargs.Value = value
	pargs.Version = version
	var preply rpc.PutReply

	firstTry := true
	for {
		ok := ck.clnt.Call(ck.server, "KVServer.Put", &pargs, &preply)
		if ok {
			// RPC 成功，检查服务器返回的错误
			if preply.Err == rpc.OK || preply.Err == rpc.ErrNoKey {
				return preply.Err
			} else if preply.Err == rpc.ErrVersion {
				if firstTry {
					// 第一次尝试返回 ErrVersion，说明版本不匹配
					return rpc.ErrVersion
				} else {
					// 重试时返回 ErrVersion，可能第一次已经成功了
					return rpc.ErrMaybe
				}
			}
		}
		// RPC 失败或其他错误，继续重试
		firstTry = false
	}
}
