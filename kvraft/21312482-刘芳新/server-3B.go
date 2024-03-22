package kvraft

import (
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

// Debug 控制是否打印调试信息
const Debug = false

// DPrintf 决定是否打印调试信息
func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

// Op 定义了 KV 服务的操作类型
type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key       string
	Value     string
	Operation string
	Id        int64
}

// List 用于记录操作的列表结构
type List struct {
	Data []interface{}
	Len  int
}

// Push 将元素压入列表
func (list *List) Push(args interface{}) {
	list.Data = append(list.Data, args)
	list.Len = len(list.Data)
}

// Pop 弹出列表中的第一个元素
func (list *List) Pop() (result interface{}) {
	result = list.Data[0]
	list.Data = list.Data[1:]
	list.Len = len(list.Data)
	return
}

// KVServer 表示键值存储服务器
type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()  1 表示服务器已终止

	maxraftstate int // 达到此大小时进行快照

	// Your definitions here.
	CommandLog      map[int64]string // 记录每个客户端请求的结果
	CommandLogQueue List // 记录每个客户端请求的 Id，用于控制队列长度
	cond            *sync.Cond
	Data            map[string]string // 存储键值对
}

// Get 处理客户端的 Get 请求
func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.cond.L.Lock()
	result, ok := kv.CommandLog[args.Id]
	if ok {
		reply.Value = result
		kv.cond.L.Unlock()
		return
	}
	command := Op{Key: args.Key, Operation: "Get", Id: args.Id}
	kv.cond.L.Unlock()
	_, term, ok := kv.rf.Start(command)
	if !ok {
		reply.Err = "Not leader"
		return
	}
	var timerController bool = true
	var timeoutTime = 0
	kv.cond.L.Lock()
	for {
        // 判断是否请求被完成
		result, ok = kv.CommandLog[args.Id]
		if ok {
			reply.Value = result
			timerController = false
			kv.cond.L.Unlock()
			return
		}
		nowTerm, isLeder := kv.rf.GetState()
        // 请求并未完成，但是发现本raft节点已经不是leader节点，或者是等待请求结果已经超时3次了，
		// 或者发现本节点的状态已经和当时发起共识时不一样了则此次动作作废，以上几种情况均是失败的，直接返回共识错误给客户端
		if nowTerm != term || !isLeder || timeoutTime == 3 {
			reply.Err = "Consensus error"
			timerController = false
			kv.cond.L.Unlock()
			return
		}
		go func() {
            // 计时器线程，每经过100ms都没有完成请求的执行就由计时器来自动唤醒，查看是什么情况，若超过3次自动唤醒都没有完成，则直接终止，对应上面
			time.Sleep(time.Duration(100) * time.Millisecond)
			kv.cond.L.Lock()
			if timerController {
				timeoutTime++
				kv.cond.L.Unlock()
				kv.cond.Broadcast()
			} else {
				kv.cond.L.Unlock()
				return
			}
		}()
		kv.cond.Wait()
	}
}

// PutAppend 处理客户端的 Put 或 Append 请求
func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.cond.L.Lock()
	_, ok := kv.CommandLog[args.Id]
	if ok {
		kv.cond.L.Unlock()
		return
	}
	command := Op{Key: args.Key, Operation: args.Op, Value: args.Value, Id: args.Id}
	kv.cond.L.Unlock()
	_, term, ok := kv.rf.Start(command)
	if !ok {
		reply.Err = "Not leader"
		return
	}
	var timerController bool = true
	var timeoutTime = 0
	kv.cond.L.Lock()
	for {
		_, ok = kv.CommandLog[args.Id]
		if ok {
			timerController = false
			kv.cond.L.Unlock()
			return
		}
		nowTerm, isLeder := kv.rf.GetState()
		if nowTerm != term || !isLeder || timeoutTime == 3 {
			reply.Err = "Consensus error"
			timerController = false
			kv.cond.L.Unlock()
			return
		}
		go func() {
			time.Sleep(time.Duration(100) * time.Millisecond)
			kv.cond.L.Lock()
			if timerController {
				timeoutTime++
				kv.cond.L.Unlock()
				kv.cond.Broadcast()
			} else {
				kv.cond.L.Unlock()
				return
			}
		}()
		kv.cond.Wait()
	}
}

// executeThread 处理 Raft 模块传递过来的 ApplyMsg
func (kv *KVServer) executeThread() {
	for {
		// var command raft.ApplyMsg
		var command = <-kv.applyCh
		kv.mu.Lock()
		if command.SnapshotValid {
			// 从快照中恢复数据
			r := bytes.NewBuffer(command.Snapshot)
			d := labgob.NewDecoder(r)
			var data map[string]string
			var commandLog map[int64]string
			var commandLogQueue List
			if d.Decode(&data) != nil || d.Decode(&commandLog) != nil || d.Decode(&commandLogQueue) != nil {
				log.Printf("									Error: server%d readSnapshot.", kv.me)
			} else {
				kv.Data = data
				kv.CommandLog = commandLog
				kv.CommandLogQueue = commandLogQueue
			}
			kv.mu.Unlock()
			continue
		} else if command.CommandValid {
			var msg = command.Command.(Op)
			_, ok1 := kv.CommandLog[msg.Id]
			// 如果请求 Id 已经存在于 CommandLog 中，说明该请求已经被执行过
			if !ok1 {
				if msg.Operation == "Get" {
					result, ok2 := kv.Data[msg.Key]
					if ok2 {
						msg.Value = result
					} else {
						msg.Value = ""
					}
				}
				if msg.Operation == "Put" {
					kv.Data[msg.Key] = msg.Value
				}
				if msg.Operation == "Append" {
					kv.Data[msg.Key] += msg.Value
				}
				// 控制 CommandLogQueue 的长度
				if kv.CommandLogQueue.Len >= 5 {
					kv.CommandLogQueue.Pop()
				}
				kv.CommandLogQueue.Push(msg.Id)
				kv.CommandLog[msg.Id] = msg.Value

				// 如果达到 maxraftstate，进行快照
				var statesize, snapshotIndex = kv.rf.GetStateSizeAndSnapshotIndex()
				if kv.maxraftstate != -1 && statesize >= kv.maxraftstate && command.CommandIndex-snapshotIndex >= 20 {
					// the size of raftstate is approaching maxraftstate
					w := new(bytes.Buffer)
					e := labgob.NewEncoder(w)
					var encodeCommandLog = make(map[int64]string, 5)
					for i := 0; i < kv.CommandLogQueue.Len; i++ {
						var msgId = kv.CommandLogQueue.Data[i].(int64)
						encodeCommandLog[msgId] = kv.CommandLog[msgId]
					}
					e.Encode(kv.Data)
					e.Encode(encodeCommandLog)
					e.Encode(kv.CommandLogQueue)
					data := w.Bytes()
					kv.rf.Snapshot(command.CommandIndex, data)
				}
			}
			kv.mu.Unlock()
			kv.cond.Broadcast()
		}
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
// Kill 标记服务器已经终止
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

// killed 返回服务器是否已经终止
func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
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
//
// StartKVServer 启动 KV 服务器
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{}) // 在 Go 的 RPC 库中注册 Op 结构

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.CommandLog = make(map[int64]string)
	kv.cond = sync.NewCond(&kv.mu)
	kv.Data = make(map[string]string)
	kv.CommandLogQueue = List{}
	var snapshot = kv.rf.GetSnapshot() // 从快照中恢复数据
	if len(snapshot) != 0 {
		r := bytes.NewBuffer(snapshot)
		d := labgob.NewDecoder(r)
		var data map[string]string
		var commandLog map[int64]string
		var commandLogQueue List
		if d.Decode(&data) != nil || d.Decode(&commandLog) != nil || d.Decode(&commandLogQueue) != nil {
		} else {
			kv.Data = data
			kv.CommandLog = commandLog
			kv.CommandLogQueue = commandLogQueue
		}
	}
	go kv.executeThread() // 启动执行线程
	// You may need initialization code here.

	return kv
}
