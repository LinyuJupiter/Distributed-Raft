package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
	
	// "6.824/labgob"
	"6.824/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
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

// LogEntry 结构定义了 Raft 日志的条目
type LogEntry struct {
}


// HeartBeatInterval 定义心跳间隔，单位为毫秒
var HeartBeatInterval = 100 // ms

// TimeOutInterval 定义选举超时间隔，单位为毫秒
var TimeOutInterval = 210 // ms

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state 用于保护对节点状态的共享访问的锁
	peers     []*labrpc.ClientEnd // RPC end points of all peers      					所有节点的 RPC 终端点
	persister *Persister          // Object to hold this peer's persisted state 	    用于保存节点持久状态的对象
	me        int                 // this peer's index into peers[] // 					节点在 peers[] 中的索引
	dead      int32               // set by Kill()										由 Kill() 设置

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// persistent state
	peerNum           int   // 节点数量
	currentTerm       int   // 当前任期
	voteFor           int   // 投票给哪个节点
	log               []LogEntry // 日志条目

	// volatile state
	commitIndex int // 已经被提交的日志的最大索引
	lastApplied int // 已经被应用到状态机的最大索引
	state       string // 节点状态
	lastLogIndex int // 最后一个日志条目的索引
	lastLogTerm  int // 最后一个日志条目的任期


	// Candidate 使用条件变量同步选举
	mesMutex    sync.Mutex // 用于锁定变量 ElectionStatus
	messageCond *sync.Cond // 条件变量

	// ElectionStatus == 1 -> 开始选举
	// ElectionStatus == -1 -> 保持不动
	// ElectionStatus == 2 -> 成为领导者
	// ElectionStatus == 3 -> 选举超时
	ElectionStatus int

	// volatile state on leaders
	nextIndex  []int // 用于每个节点的下一个日志条目的索引
	matchIndex []int // 用于每个节点已匹配的最高日志条目的索引
}

// return currentTerm and whether this server
// believes it is the leader.
// GetState 返回当前节点的任期和该节点是否认为自己是领导者。
func (rf *Raft) GetState() (int, bool) {

	var term int        // 当前节点的任期
	var isleader bool   // 当前节点是否认为自己是领导者
	// Your code here (2A).
	// 使用互斥锁保护共享状态，确保在读取状态期间不会发生竞态条件
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state == "leader" {
		isleader = true
	}
	term = rf.currentTerm
	return term, isleader
}

// changeElectionStatus 用于更改节点的ElectionStatus
func (rf *Raft) changeElectionStatus(num int) {
	rf.mesMutex.Lock()
	rf.ElectionStatus = num
	rf.mesMutex.Unlock()
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
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

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	
}


// example RequestVote RPC arguments structure.
// field names must start with capital letters!
// RequestVoteArgs 结构定义了 RequestVote RPC 请求的参数。
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term int // 当前候选者的任期
	CandidateId int // 请求投票的候选者的ID
	LastLogIndex int // 候选者的最后一个日志条目的索引
	LastLogTerm int // 候选者的最后一个日志条目的任期
}

// AppendEntriesArgs 结构定义了 AppendEntries RPC 请求的参数。
type AppendEntriesArgs struct {
	Term int // 领导者的任期
	LeaderId int // 领导者的ID
	PrevLogIndex int // 前一个日志条目的索引
	PrevLogTerm int // 前一个日志条目的任期
	Entries []LogEntry // 要追加到日志中的日志条目
	LeaderCommit int // 领导者已经提交的日志的最大索引
}


// example RequestVote RPC reply structure.
// field names must start with capital letters!
// RequestVoteReply 结构定义了 RequestVote RPC 响应的参数。
type RequestVoteReply struct {
	Term int // 对候选者投票的节点的当前任期
	VoteGrand bool // 是否授予投票，如果为 true，则表示投票成功
}

// AppendEntriesReply 结构定义了 AppendEntries RPC 响应的参数。
type AppendEntriesReply struct {
	Term int // 节点的当前任期
	ConflictIndex int // 冲突的日志条目的索引，如果有冲突
	Success bool // 如果追加成功，则为 true
}


// example RequestVote RPC handler.
// RequestVote 处理来自其他节点的 RequestVote RPC 请求。
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).

	// 使用互斥锁保护对节点状态的共享访问，确保在处理请求期间不会发生竞态条件
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 设置响应中的当前任期为节点的当前任期
	reply.Term = rf.currentTerm

	// 如果请求的任期小于当前任期，则拒绝投票
	if args.Term < rf.currentTerm {
		reply.VoteGrand = false
	}

	// 如果请求的最后一个日志条目的任期小于节点的最后一个日志条目的任期，则拒绝投票
	if args.LastLogTerm < rf.lastLogTerm {
		// 如果请求的任期大于当前任期，则更新当前任期，并转变为 follower 状态
		if args.Term > rf.currentTerm {
			rf.currentTerm = args.Term
			rf.voteFor = -1
			rf.changeElectionStatus(-1)
			rf.state = "follower"
			rf.messageCond.Broadcast()
		}
		reply.VoteGrand = false
	} else if args.LastLogTerm > rf.lastLogTerm {
		// 如果请求的最后一个日志条目的任期大于节点的最后一个日志条目的任期，则授予投票
		if rf.currentTerm == args.Term && rf.voteFor != -1 {
			// 如果节点已经投过票给其他节点，则拒绝投票
			reply.VoteGrand = false
		} else {
			// 否则，更新当前任期，投票给请求的候选者，并转变为 follower 状态
			if rf.currentTerm < args.Term {
				rf.currentTerm = args.Term
			}
			rf.voteFor = args.CandidateId
			reply.VoteGrand = true
			rf.changeElectionStatus(-1)
			rf.state = "follower"
			rf.messageCond.Broadcast()
		}
	} else {
		// 如果最后一个日志条目的任期相等，则需要进一步比较索引
		if args.LastLogIndex >= rf.lastLogIndex {
			if rf.currentTerm == args.Term && rf.voteFor != -1 {
				// 如果节点已经投过票给其他节点，则拒绝投票
				reply.VoteGrand = false
			} else {
				// 否则，更新当前任期，投票给请求的候选者，并转变为 follower 状态
				if rf.currentTerm < args.Term {
					rf.currentTerm = args.Term
				}
				rf.voteFor = args.CandidateId
				reply.VoteGrand = true
				rf.changeElectionStatus(-1)
				rf.state = "follower"
				rf.messageCond.Broadcast()
			}
		} else {
			// 如果最后一个日志条目的索引小于节点的最后一个日志条目的索引，则拒绝投票
			reply.VoteGrand = false
			// 如果请求的任期大于当前任期，则更新当前任期，并转变为 follower 状态
			if args.Term > rf.currentTerm {
				rf.currentTerm = args.Term
				rf.voteFor = -1
				rf.changeElectionStatus(-1)
				rf.state = "follower"
				rf.messageCond.Broadcast()
			}
		}
	}
}

// AppendEntries 处理来自领导者的 AppendEntries RPC 请求
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm // 设置响应中的当前任期为节点的当前任期
	if args.Term < rf.currentTerm { // 如果请求的任期小于当前任期，则拒绝追加日志条目
		reply.Success = false
	} else {
		reply.Success = true
		// 更新节点的当前任期，并转变为 follower 状态
		rf.currentTerm = args.Term
		rf.state = "follower"
		rf.changeElectionStatus(-1)
		rf.messageCond.Broadcast()
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

	// Your code here (2B).


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

// heartBeats 用于定期发送心跳消息以维持领导者地位。
// 这个函数在一个独立的协程中运行，定期向其他节点发送心跳消息。
// 参数 ch 是一个通道，用于在函数执行完毕时发送信号。
func (rf *Raft) heartBeats(ch chan int) {
	// 加锁，读取当前节点的当前任期
	rf.mu.Lock()
	var term = rf.currentTerm
	rf.mu.Unlock()
	// 循环执行心跳消息的发送
	for rf.killed() == false {
		// 获取当前节点的状态和是否为领导者
		_, isLeader := rf.GetState()
		if !isLeader {
			// 如果当前节点不是领导者，则退出循环，停止发送心跳消息
			break
		}
		// 加锁，读取当前节点的信息
		rf.mu.Lock()
		var peerNum = rf.peerNum
		var args AppendEntriesArgs
		args.LeaderId = rf.me
		args.Term = rf.currentTerm
		rf.mu.Unlock()
		// 遍历所有节点，向它们发送心跳消息
		for index := 0; index < peerNum; index++ {
			if index == rf.me {
				continue
			}
			// 启动协程发送心跳消息
			go func(index int) {
				// 初始化 AppendEntries 回复
				var reply AppendEntriesReply
				// 发送心跳消息
				if rf.sendAppendEntries(index, &args, &reply) {
					// 加锁，处理心跳回复
					rf.mu.Lock()
					defer rf.mu.Unlock()
					// 检查回复的任期是否大于当前节点的任期
					if reply.Term > term {
						// 如果回复的任期大于当前节点的任期，更新当前节点的状态为 follower
						if reply.Term > rf.currentTerm {
							rf.state = "follower"
							rf.currentTerm = reply.Term
							// 更新投票状态，重置投票
							rf.voteFor = -1
						}
					}
				}
			}(index)
		}
		time.Sleep(time.Duration(HeartBeatInterval) * time.Millisecond)
	}
	ch <- 1
}


// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
// ticker 用于在节点成为 follower 时启动计时器，以监测是否需要发起选举。
// 该函数在一个独立的协程中运行，定期检查是否需要启动选举，以及通过随机睡眠来模拟计时器。
func (rf *Raft) ticker() {
	rf.mu.Lock() // 加锁，读取当前节点的当前任期
	var recordTerm = rf.currentTerm
	rf.mu.Unlock()
	// 初始化互斥锁和计数器，用于确保唯一性
	var timeMutex sync.Mutex
	var currentTermTimes = 0
	// 循环执行计时器的逻辑
	for rf.killed() == false {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		// 在这里检查是否应该启动选举，并通过 time.Sleep() 随机睡眠一段时间
		rf.mu.Lock()
		timeMutex.Lock()
		if recordTerm != rf.currentTerm {
			recordTerm = rf.currentTerm
			currentTermTimes = 0
		}
		currentTermTimes++

		rand.Seed(time.Now().UnixNano()) // 生成随机的睡眠时间
		sleepTime := rand.Intn(TimeOutInterval) + TimeOutInterval
		rf.changeElectionStatus(0) // 初始化 ElectionStatus 变量，用于测试是否超时
		go func(currentTerm int, cTermTimes int) { // 启动协程，用于检测是否超时
			time.Sleep(time.Duration(sleepTime) * time.Millisecond)
			rf.mu.Lock()
			defer rf.mu.Unlock()
			rf.mesMutex.Lock()
			defer rf.mesMutex.Unlock()
			timeMutex.Lock()
			defer timeMutex.Unlock()
			// 如果 ElectionStatus 不为 -1 且当前任期和计数器未更改，则将 ElectionStatus 设置为 1 并广播条件变量
			if rf.ElectionStatus != -1 && currentTerm == rf.currentTerm && cTermTimes == currentTermTimes {
				rf.ElectionStatus = 1
				rf.messageCond.Broadcast()
			}
		}(rf.currentTerm, currentTermTimes)
		// 如果接收到附加日志或请求投票消息，则 ElectionStatus 变为 -1；如果超时，ElectionStatus 变为 1
		rf.mu.Unlock()
		timeMutex.Unlock()
		rf.mesMutex.Lock()
		rf.messageCond.Wait() // 等待 ElectionStatus 变为 -1 或 1
		for rf.ElectionStatus == 2 || rf.ElectionStatus == 3 {
			rf.messageCond.Wait()
		}
		// 如果 ElectionStatus 为 -1，则表示接收到了有效的消息并重置了计时器
		if rf.ElectionStatus == -1 {
			rf.mesMutex.Unlock()
			continue
		} else if rf.ElectionStatus == 1 {
			// 开始选举
			rf.mesMutex.Unlock()
			// 启动两个线程
			// 一个线程是计时器，检查选举是否超时
			// 另一个线程负责启动选举
		voteLoop:
			for rf.killed() == false {
				// 如果超时，发起新的选举
				// ch 用于确定选举是否成功或超时
				rf.mu.Lock()
				rf.state = "candidate"
				rf.currentTerm++
				rf.voteFor = rf.me
				var voteArgs = &RequestVoteArgs{}
				voteArgs.Term = rf.currentTerm
				voteArgs.LastLogTerm = rf.lastLogTerm
				voteArgs.LastLogIndex = rf.lastLogIndex
				voteArgs.CandidateId = rf.me

				var voteMutex sync.Mutex
				var voteCond = sync.NewCond(&voteMutex)

				rf.changeElectionStatus(0)
				// 启动协程发起选举
				go func(currentTerm int, currentTermTimes int, pNum int) {
					// 启动选举
					var grandNum = 1
					var refuseNum = 0
					var term = currentTerm
					var times = currentTermTimes
					var peerNum = pNum
					// 遍历所有节点，向它们发送请求投票消息
					for index := 0; index < peerNum; index++ {
						if index == rf.me {
							continue
						}
						go func(index int) {
							var reply = &RequestVoteReply{}
							if rf.sendRequestVote(index, voteArgs, reply) {
								rf.mu.Lock()
								if reply.Term > rf.currentTerm {
									// 如果回复的任期大于当前任期，则更新节点状态为 follower
									if term == rf.currentTerm {
										rf.currentTerm = reply.Term
										rf.state = "follower"
										// 发现更高的任期并更新节点状态
										rf.mu.Unlock()
										rf.changeElectionStatus(4)
										rf.messageCond.Broadcast()
										voteCond.Broadcast()
										return
									} else {
										rf.currentTerm = reply.Term
										rf.state = "follower"
										// 标记陈旧的投票请求发现更高的任期并更新节点状态
										rf.mu.Unlock()
										rf.changeElectionStatus(-1)
										rf.messageCond.Broadcast()
										return
									}
								}
								rf.mu.Unlock()
								if reply.VoteGrand { // 处理投票回复
									// 接收到其他节点的投票
									voteCond.L.Lock()
									grandNum++
									voteCond.L.Unlock()
									voteCond.Broadcast()
								} else {
									// 接收到其他节点的拒绝
									voteCond.L.Lock()
									refuseNum++
									voteCond.L.Unlock()
									voteCond.Broadcast()
								}
							}
						}(index)
					}
					// 等待投票结果
					var voteSuccess = false
					for {
						voteCond.L.Lock()
						voteCond.Wait()
						rf.mu.Lock()
						if rf.state == "follower" {
							rf.mu.Unlock()
							voteCond.L.Unlock()
							return
						}
						// 获取节点的总数
						var peerNum = rf.peerNum
						rf.mu.Unlock()
						// 判断是否成功获取超过半数的投票
						if grandNum >= (peerNum/2)+1 {
							voteSuccess = true
							// 成功获取大多数的投票
							voteCond.L.Unlock()
							break
						} else if refuseNum+grandNum == peerNum || refuseNum >= (peerNum/2)+1 {
							// 没有获取到大多数的投票
							voteCond.L.Unlock()
							break
						}
						voteCond.L.Unlock()
					}
					// 如果选举成功，更新节点状态为 leader
					if voteSuccess {
						rf.mu.Lock()
						defer rf.mu.Unlock()
						rf.mesMutex.Lock()
						defer rf.mesMutex.Unlock()
						timeMutex.Lock()
						defer timeMutex.Unlock()
						// 如果 ElectionStatus 不为 -1 且不为 4，且当前任期和计数器未更改，则将 ElectionStatus 设置为 2 并广播条件变量
						if rf.ElectionStatus != -1 && rf.ElectionStatus != 4 && term == rf.currentTerm && times == currentTermTimes {
							// 选举成功表示节点成功成为领导者
							rf.ElectionStatus = 2
							rf.state = "leader"
							rf.messageCond.Broadcast()
						}
						// 如果一个节点收集了正好半数的投票，它将等待直到超时并启动下一次选举
					}
				}(rf.currentTerm, currentTermTimes, rf.peerNum)
				// 启动协程，用于等待选举超时
				go func(currentTerm int, cTermTimes int) {
					rand.Seed(time.Now().UnixNano())
					sleepTime := rand.Intn(TimeOutInterval) + TimeOutInterval
					// 等待选举超时
					time.Sleep(time.Duration(sleepTime) * time.Millisecond)
					rf.mu.Lock()
					defer rf.mu.Unlock()
					rf.mesMutex.Lock()
					defer rf.mesMutex.Unlock()
					timeMutex.Lock()
					defer timeMutex.Unlock()
					// 如果 ElectionStatus 不为 -1 且不为 4，且当前任期和计数器未更改，则将 ElectionStatus 设置为 3 并广播条件变量
					if rf.ElectionStatus != -1 && 
					rf.ElectionStatus != 4 && 
					currentTerm == rf.currentTerm && 
					cTermTimes == currentTermTimes {
						rf.ElectionStatus = 3
						rf.messageCond.Broadcast()
					}
				}(rf.currentTerm, currentTermTimes)
				// 解锁，等待条件变量
				rf.mu.Unlock()
				rf.mesMutex.Lock()
				rf.messageCond.Wait()
				// 等待 ElectionStatus 变为 1 或 0
				for rf.ElectionStatus == 1 || rf.ElectionStatus == 0 {
					rf.messageCond.Wait()
				}
				// 根据 ElectionStatus 的值采取不同的行动
				switch rf.ElectionStatus {
				case 2: // 如果 ElectionStatus 为 2，表示节点成为领导者
					rf.mesMutex.Unlock() // 解锁消息互斥锁，以便让 leader 发送心跳
					// 启动心跳协程
					var leaderch = make(chan int)
					go rf.heartBeats(leaderch)
					<-leaderch
					break voteLoop // 退出选举循环 voteLoop
				case 3: // 如果 ElectionStatus 为 3，表示选举超时
					rf.mesMutex.Unlock() // 解锁消息互斥锁，重新开始选举
					continue
				case 4:
					fallthrough
				case -1:
					// 如果 ElectionStatus 为 4 或 -1，表示停止选举
    				// 解锁消息互斥锁，并退出选举循环 voteLoop
					rf.mesMutex.Unlock()
					break voteLoop
				}
			}
		}
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
// Make 用于创建并初始化 Raft 节点的实例。
// 参数 peers 表示所有 Raft 节点的客户端端点，
// 参数 me 表示当前节点的标识，
// 参数 persister 为持久化存储对象，用于保存 Raft 节点状态和日志条目，
// 参数 applyCh 为应用层传入的通道，用于接收已提交的日志条目。
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{} // 创建一个 Raft 实例 rf
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.messageCond = sync.NewCond(&rf.mesMutex) // 初始化消息条件变量
	// rf.voteCond = sync.NewCond(&rf.mu)
	rf.peerNum = len(peers)
	rf.voteFor = -1

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState()) // 从持久化存储中恢复节点状态和日志

	// start ticker goroutine to start elections
	go rf.ticker() // 启动 ticker 协程，定期检查是否需要发起选举

	return rf
}
