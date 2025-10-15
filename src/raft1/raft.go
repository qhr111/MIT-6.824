package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	//	"bytes"

	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

const (
	FOLLOWER  = "follower"
	CANDIDATE = "candidate"
	LEADER    = "leader"
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu         sync.Mutex          // Lock to protect shared access to this peer's state
	peers      []*labrpc.ClientEnd // RPC end points of all peers
	persister  *tester.Persister   // Object to hold this peer's persisted state
	me         int                 // this peer's index into peers[]
	dead       int32               // set by Kill()
	state      string
	checkAlive bool

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int        // 当前任期号， 初始化为0， 只增不减
	voteFor     int        // 当前任期内投票给了哪个候选人， -1代表没投过票
	log         []logEntry // 日志条目， 每个条目包含了一个用户状态机执行的命令， 和该条目对应的任期号

	commitIndex int // 已提交的最高logentry的索引
	lastApplied int // 已经应用到状态机的最高logentry的索引

	nextIndex  []int // 存储发送给不同server的下一个logentry的索引
	matchIndex []int // 存储不同server已经复制的最高logentry的索引
}

type logEntry struct {
	Term    int    // 该logentry所属的任期
	Command string // 状态机要执行的命令
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (3A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	if rf.state == LEADER {
		isleader = true
	} else {
		isleader = false
	}

	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	//如果对方的term小于自己的， 直接拒绝
	//如果对方的term大于自己的， 直接投票给对方
	//如果对方的term和自己一样， 但是自己没投过票或者投给了对方， 并且对方的log比自己新， 投票给对方
	//否则拒绝

	// 假设raftState的第一个int是currentTerm， 第二个int是voteFor
	log.Printf("RequestVote is called")
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	} else if (args.Term > rf.currentTerm) || (args.Term == rf.currentTerm && (rf.voteFor == -1 || rf.voteFor == args.CandidateId) && args.LastLogIndex >= rf.lastApplied) {
		reply.Term = rf.currentTerm
		reply.VoteGranted = true
		return
	}
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

type AppendEntriesArgs struct {
	term         int
	leaderId     int
	prevLogIndex int
	prevLogTerm  int
	entries      []logEntry //对于心跳包来说是空的
	leaderCommit int
}

type AppendEntriesReply struct {
	term    int
	success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (3A, 3B).
	// 如果对方的term小于自己的， 直接拒绝
	// 需要一致性检查， 判断本server在prevLogIndex索引处是否已有日志，有了的话是否term一致，不一致则拒绝
	log.Printf("AppendEntries is called")
	rf.mu.Lock()
	defer rf.mu.Unlock()

	//重置定时器ticker
	rf.checkAlive = true

	if args.term < rf.currentTerm || len(rf.log) <= args.prevLogIndex || rf.log[args.prevLogIndex].Term != args.prevLogTerm {
		if rf.log[args.prevLogIndex].Term != args.prevLogTerm {
			// 日志不一致， 删除本地冲突的日志及其之后的所有日志
			rf.log = rf.log[:args.prevLogIndex]
		}
		reply.term = rf.currentTerm
		reply.success = false
		return
	} else {
		rf.log = append(rf.log, args.entries...)
		reply.term = rf.currentTerm
		reply.success = true

		// 修改commitIndex
		if args.leaderCommit > rf.commitIndex {
			if args.leaderCommit < len(rf.log)-1 {
				rf.commitIndex = args.leaderCommit
			} else {
				rf.commitIndex = len(rf.log) - 1
			}
		}
		return
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func election(rf *Raft) {
	// Your code here (3A).
	// 作为follower， 如果在一个选举周期内没有收到leader的心跳包， 就开始选举
	log.Printf("Server %d start election\n", rf.me)

	rf.state = CANDIDATE
	rf.currentTerm += 1
	rf.voteFor = rf.me
	rf.checkAlive = true

	voteCount := 1
	for i := range rf.peers {
		if i != rf.me {
			var args RequestVoteArgs
			args.Term = rf.currentTerm
			args.CandidateId = rf.me
			args.LastLogIndex = len(rf.log) - 1
			args.LastLogTerm = rf.log[len(rf.log)-1].Term

			var reply RequestVoteReply
			go func(i int, args RequestVoteArgs, reply *RequestVoteReply) {
				//可能丢包， 要重试？
				for {
					log.Printf("Server %d start send RequestVote to server %d\n", rf.me, i)
					ok := rf.sendRequestVote(i, &args, reply)
					log.Printf("Server %d receive reply from server %d, ok: %t\n", rf.me, i, ok)
					if ok {
						rf.mu.Lock()
						defer rf.mu.Unlock()
						if reply.VoteGranted {
							voteCount += 1
							if voteCount > len(rf.peers)/2 && rf.state == CANDIDATE {
								// 赢得选举， 成为leader
								rf.state = LEADER
								// 初始化nextIndex和matchIndex
								for i := range rf.peers {
									rf.nextIndex[i] = len(rf.log)
									rf.matchIndex[i] = 0
								}
								break
							}
						} else {
							if reply.Term > rf.currentTerm {
								// 对方的term更大， 自己变成follower
								rf.state = FOLLOWER
								rf.currentTerm = reply.Term
								rf.voteFor = -1
								break
							}
						}
					}
					// 重试
				}
			}(i, args, &reply)
		}
	}
}

func (rf *Raft) ticker() {
	for !rf.killed() {

		// Your code here (3A)
		// Check if a leader election should be started.
		rf.mu.Lock()
		if rf.state == LEADER {
			rf.mu.Unlock()
		}
		if rf.checkAlive {
			rf.checkAlive = false
			rf.mu.Unlock()
		} else {
			// 没有收到心跳包， 开始选举
			election(rf)
			rf.mu.Unlock()
		}

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) sendHeartBeats() {
	// Your code here (3A).
	// 作为leader， 需要定期给其他server发送心跳包
	// 心跳包其实就是不带entries的AppendEntries RPC
	for rf.state == LEADER && !rf.killed() {
		rf.mu.Lock()
		defer rf.mu.Unlock()

		var args AppendEntriesArgs
		args.term = rf.currentTerm
		args.leaderId = rf.me
		args.prevLogIndex = len(rf.log) - 1
		args.prevLogTerm = rf.log[len(rf.log)-1].Term
		args.entries = []logEntry{}
		args.leaderCommit = rf.commitIndex

		for i := range rf.peers {
			if i != rf.me {
				var reply AppendEntriesReply
				go rf.sendAppendEntries(i, &args, &reply)
			}
		}
		time.Sleep(150 * time.Millisecond)
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.state = FOLLOWER
	rf.checkAlive = false

	// Your initialization code here (3A, 3B, 3C).
	rf.currentTerm = 0
	rf.voteFor = -1
	rf.log = make([]logEntry, 1) // log的第一个元素不存储任何有用信息， 只是为了让log的索引从1开始
	rf.log[0] = logEntry{
		Term:    0,
		Command: "",
	}

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	// start ticker gorountine to send heartbeat
	go rf.sendHeartBeats()

	return rf
}
