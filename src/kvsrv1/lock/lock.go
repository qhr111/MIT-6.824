package lock

import (
	"log"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
)

const (
	LOCKED   = "locked"
	UNLOCKED = "unlocked"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck kvtest.IKVClerk
	// You may add code here
	state string
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// Use l as the key to store the "lock state" (you would have to decide
// precisely what the lock state is).
func MakeLock(ck kvtest.IKVClerk, l string) *Lock {
	lk := &Lock{ck: ck}
	// You may add code here
	lk.state = UNLOCKED
	return lk
}

func (lk *Lock) Acquire() {
	// Your code here
	if lk.state == LOCKED {
		return
	}
	for {
		val, ver, err := lk.ck.Get("l")
		switch {
		case err == rpc.ErrNoKey:
			// 锁对象不存在，创建一个
			err = lk.ck.Put("l", LOCKED, 0)
			if err == rpc.OK || err == rpc.ErrMaybe {
				lk.state = LOCKED
				return
			}
		case err == rpc.OK:
			// 锁对象存在
			if val == UNLOCKED {
				// 锁是空闲的，尝试获取锁
				err = lk.ck.Put("l", LOCKED, ver)
				if err == rpc.OK || err == rpc.ErrMaybe {
					lk.state = LOCKED
					return
				}
			}
		default:
			// 其他错误
			log.Printf("Acquire error, unexpected err： %s", err)
		}
	}
}

func (lk *Lock) Release() {
	// Your code here
	// 如何实现at-most-once语义？
	if lk.state == UNLOCKED {
		return
	}
	for {
		val, ver, err := lk.ck.Get("l")
		switch {
		case err == rpc.OK:
			{
				if val == LOCKED {
					// 锁是被占用的，尝试释放锁
					err = lk.ck.Put("l", UNLOCKED, ver)
					if err == rpc.OK || err == rpc.ErrMaybe {
						lk.state = UNLOCKED
					}
					if err == rpc.ErrVersion {
						//如果重试，别的client是没办法释放的， 既然版本号变了， 说明之前一定成功释放了， 直接修改状态
						lk.state = UNLOCKED
					}
				} else {
					// 锁已经是空闲的了
					lk.state = UNLOCKED
					log.Printf("Release error, lock is already UNLOCKED")
				}
				return
			}
		case err == rpc.ErrNoKey:
			{
				log.Printf("release() error! unexpected err: %s", err)
				return
			}
		default:
			{
				log.Printf("release() error! unexpected err: %s", err)
				return
			}
		}
	}
}
