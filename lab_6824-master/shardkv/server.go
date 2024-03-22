package shardkv

import (
	"bytes"
	"log"
	"sync/atomic"
	"time"

	"sync"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"6.824/shardctrler"
)


const (
	UpConfigLoopInterval = 100 * time.Millisecond // 轮询配置的时间间隔
	GetTimeout          = 500 * time.Millisecond // Get 操作的超时时间
	AppOrPutTimeout     = 500 * time.Millisecond // Append 或 Put 操作的超时时间
	UpConfigTimeout     = 500 * time.Millisecond // 更新配置的超时时间
	AddShardsTimeout    = 500 * time.Millisecond // 添加分片的超时时间
	RemoveShardsTimeout = 500 * time.Millisecond // 移除分片的超时时间
)

// Shard 结构表示一个分片，用于存储键值对
type Shard struct {
	KvMap     map[string]string // 键值映射
	ConfigNum int               // 分片所在配置的版本号
}

// Op 结构表示一个操作，用于在 Raft 日志中传递
type Op struct {
	ClientId int64      // 客户端标识
	SeqId    int        // 操作序号
	OpType   Operation  // 操作类型："get"、"put"、"append"
	Key      string     // 键
	Value    string     // 值
	UpConfig shardctrler.Config // 更新的配置信息
	ShardId  int        // 分片标识
	Shard    Shard      // 分片数据
	SeqMap   map[int64]int // 客户端序号映射表
}

// OpReply 用于唤醒等待 RPC 调用的客户端，当 Op 从 applyCh 中到达时使用
type OpReply struct {
	ClientId int64 // 客户端标识
	SeqId    int   // 操作序号
	Err      Err   // 操作的结果
}

// ShardKV 结构表示 ShardKV 服务器
type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	makeEnd      func(string) *labrpc.ClientEnd
	gid          int
	masters      []*labrpc.ClientEnd
	maxRaftState int // 当日志增长到一定大小时触发快照
	// Your definitions here.
	dead int32 // 用于标记服务器是否停止

	Config     shardctrler.Config // 当前需要更新的最新配置
	LastConfig shardctrler.Config // 更新之前的配置，用于比对是否全部更新完成

	shardsPersist []Shard // 分片数组，ShardId -> Shard；如果 KvMap 为 nil，则表示当前数据不归当前分片管理
	waitChMap     map[int]chan OpReply // 用于存储等待 Op 结果的通道映射
	SeqMap        map[int64]int        // 客户端序号映射表
	sck           *shardctrler.Clerk  // sck 是一个用于与 Shard Master 通信的客户端
}



// applyMsgHandlerLoop 处理 applyCh 发送过来的 ApplyMsg。
func (kv *ShardKV) applyMsgHandlerLoop() {
	for {
		// 检查当前服务器是否已经被标记为不活跃，如果是则退出循环
		if kv.killed() {
			return
		}		
		// 通过 select 语句监听 applyCh 和 killed 状态
		select {
		case msg := <-kv.applyCh:
			// 检查是否为有效的 Raft 指令
			if msg.CommandValid == true {
				kv.mu.Lock()
				// 将命令转换为 Op 结构
				op := msg.Command.(Op)
				// 构造 OpReply 结构，用于返回给客户端
				reply := OpReply{
					ClientId: op.ClientId,
					SeqId:    op.SeqId,
					Err:      OK,
				}				
				// 判断命令类型
				if op.OpType == PutType || op.OpType == GetType || op.OpType == AppendType {
					// 处理 Put、Get 和 Append 操作
					shardId := key2shard(op.Key)					
					// 检查分片是否属于当前组
					if kv.Config.Shards[shardId] != kv.gid {
						reply.Err = ErrWrongGroup
					} else if kv.shardsPersist[shardId].KvMap == nil {
						// 如果应该存在的分片没有数据，说明该分片尚未到达
						reply.Err = ShardNotArrived
					} else {
						// 检查是否为重复请求
						if !kv.ifDuplicate(op.ClientId, op.SeqId) {
							// 更新 SeqMap 记录，处理具体操作
							kv.SeqMap[op.ClientId] = op.SeqId
							switch op.OpType {
							case PutType:
								kv.shardsPersist[shardId].KvMap[op.Key] = op.Value
							case AppendType:
								kv.shardsPersist[shardId].KvMap[op.Key] += op.Value
							case GetType:
								// 如果是 Get 操作，无需处理
							default:
								log.Fatalf("invalid command type: %v.", op.OpType)
							}
						}
					}
				} else {
					// 处理其他服务器的请求
					switch op.OpType {
					case UpConfigType:
						// 处理 UpConfig 操作
						kv.upConfigHandler(op)
					case AddShardType:
						// 处理 AddShard 操作
						// 如果当前配置号比 Op 的 SeqId 还低，说明不是最新的配置
						if kv.Config.Num < op.SeqId {
							reply.Err = ConfigNotArrived
							break
						}
						kv.addShardHandler(op)
					case RemoveShardType:
						// 处理 RemoveShard 操作
						// Remove 操作是由上一个 UpConfig 发起的
						kv.removeShardHandler(op)
					default:
						log.Fatalf("invalid command type: %v.", op.OpType)
					}
				}
				// 如果需要生成快照，并且超过了最大状态大小，则进行快照
				if kv.maxRaftState != -1 && kv.rf.GetRaftStateSize() > kv.maxRaftState {
					snapshot := kv.PersistSnapShot()
					kv.rf.Snapshot(msg.CommandIndex, snapshot)
				}

				// 将 OpReply 发送到等待的通道中
				ch := kv.getWaitCh(msg.CommandIndex)
				ch <- reply
				kv.mu.Unlock()
			}
			// 如果接收到快照消息，则进行快照的安装
			if msg.SnapshotValid == true {
				// 判断是否需要安装快照
				if kv.rf.CondInstallSnapshot(msg.SnapshotTerm, msg.SnapshotIndex, msg.Snapshot) {
					// 读取快照数据进行恢复
					kv.mu.Lock()
					kv.DecodeSnapShot(msg.Snapshot)
					kv.mu.Unlock()
				}
				continue
			}
		}
	}
}


// ConfigDetectedLoop 用于周期性检测配置变化。
func (kv *ShardKV) ConfigDetectedLoop() {
	kv.mu.Lock()
	// 获取当前服务器的配置和 Raft 实例
	curConfig := kv.Config
	rf := kv.rf
	kv.mu.Unlock()
	// 循环直到服务器被标记为不活跃
	for !kv.killed() {
		// 只有 Leader 节点需要处理配置任务
		if _, isLeader := rf.GetState(); !isLeader {
			// 如果当前节点不是 Leader，则休眠一段时间再进行下一次检测
			time.Sleep(UpConfigLoopInterval)
			continue
		}
		kv.mu.Lock()
		// 检查是否已将不属于当前组的分片分配给其他组
		if !kv.allSent() {
			SeqMap := make(map[int64]int)
			for k, v := range kv.SeqMap {
				SeqMap[k] = v
			}
			for shardId, gid := range kv.LastConfig.Shards {
				// 如果该分片属于当前组但在最新配置中不属于当前组
				if gid == kv.gid && kv.Config.Shards[shardId] != kv.gid && kv.shardsPersist[shardId].ConfigNum < kv.Config.Num {
					// 克隆该分片的数据，用于发送
					sendData := kv.cloneShard(kv.Config.Num, kv.shardsPersist[shardId].KvMap)

					// 构造发送请求的参数
					args := SendShardArg{
						LastAppliedRequestId: SeqMap,
						ShardId:              shardId,
						Shard:                sendData,
						ClientId:             int64(gid),
						RequestId:            kv.Config.Num,
					}
					// 获取当前分片的目标服务器列表
					serversList := kv.Config.Groups[kv.Config.Shards[shardId]]
					servers := make([]*labrpc.ClientEnd, len(serversList))
					for i, name := range serversList {
						servers[i] = kv.makeEnd(name)
					}
					// 开启协程，向每个目标服务器发送分片数据
					go func(servers []*labrpc.ClientEnd, args *SendShardArg) {
						index := 0
						start := time.Now()
						for {
							var reply AddShardReply
							// 调用目标服务器的 AddShard 方法，尝试将分片数据添加到目标服务器
							ok := servers[index].Call("ShardKV.AddShard", args, &reply)
							// 如果成功或者超时，都需要进行 GC 移除不再属于当前组的分片
							if ok && reply.Err == OK || time.Now().Sub(start) >= 2*time.Second {
								// 如果成功，向当前服务器发送 RemoveShard 命令进行 GC
								kv.mu.Lock()
								command := Op{
									OpType:   RemoveShardType,
									ClientId: int64(kv.gid),
									SeqId:    kv.Config.Num,
									ShardId:  args.ShardId,
								}
								kv.mu.Unlock()
								kv.startCommand(command, RemoveShardsTimeout)
								break
							}
							index = (index + 1) % len(servers)
							if index == 0 {
								time.Sleep(UpConfigLoopInterval)
							}
						}
					}(servers, &args)
				}
			}
			kv.mu.Unlock()
			time.Sleep(UpConfigLoopInterval)
			continue
		}
		// 检查是否已接收到所有分片
		if !kv.allReceived() {
			kv.mu.Unlock()
			time.Sleep(UpConfigLoopInterval)
			continue
		}
		// 当前配置已经完成，轮询下一个配置
		curConfig = kv.Config
		sck := kv.sck
		kv.mu.Unlock()
		// 向 shard master 查询下一个配置
		newConfig := sck.Query(curConfig.Num + 1)
		if newConfig.Num != curConfig.Num+1 {
			// 如果查询到的配置号不是当前配置号加一，则等待一段时间再进行下一次查询
			time.Sleep(UpConfigLoopInterval)
			continue
		}
		// 构造 UpConfig 命令
		command := Op{
			OpType:   UpConfigType,
			ClientId: int64(kv.gid),
			SeqId:    newConfig.Num,
			UpConfig: newConfig,
		}
		// 发起 UpConfig 命令，等待完成
		kv.startCommand(command, UpConfigTimeout)
	}
}



// Get 函数处理客户端的 Get 请求，根据提供的键（args.Key）获取对应的值。
func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// 根据键计算对应的分片编号
	shardId := key2shard(args.Key)
	// 上锁，确保并发安全
	kv.mu.Lock()
	// 检查当前分片是否属于该组
	if kv.Config.Shards[shardId] != kv.gid {
		reply.Err = ErrWrongGroup
	} else if kv.shardsPersist[shardId].KvMap == nil {
		// 检查分片数据是否已到达
		reply.Err = ShardNotArrived
	}
	kv.mu.Unlock()
	// 如果分片不属于该组或数据未到达，直接返回错误
	if reply.Err == ErrWrongGroup || reply.Err == ShardNotArrived {
		return
	}
	// 构造 Get 操作的命令
	command := Op{
		OpType:   GetType,
		ClientId: args.ClientId,
		SeqId:    args.RequestId,
		Key:      args.Key,
	}
	// 发送命令并等待操作完成，设置超时时间为 GetTimeout
	err := kv.startCommand(command, GetTimeout)
	if err != OK {
		reply.Err = err
		return
	}
	// 再次上锁，获取操作结果
	kv.mu.Lock()
	// 再次检查当前分片是否属于该组和分片数据是否已到达
	if kv.Config.Shards[shardId] != kv.gid {
		reply.Err = ErrWrongGroup
	} else if kv.shardsPersist[shardId].KvMap == nil {
		reply.Err = ShardNotArrived
	} else {
		// 操作成功，返回结果
		reply.Err = OK
		reply.Value = kv.shardsPersist[shardId].KvMap[args.Key]
	}
	kv.mu.Unlock()
	return
}


// PutAppend 函数处理客户端的 Put 和 Append 请求，根据提供的键（args.Key）和值（args.Value）执行相应的操作。
func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// 根据键计算对应的分片编号
	shardId := key2shard(args.Key)
	// 上锁，确保并发安全
	kv.mu.Lock()
	// 检查当前分片是否属于该组
	if kv.Config.Shards[shardId] != kv.gid {
		reply.Err = ErrWrongGroup
	} else if kv.shardsPersist[shardId].KvMap == nil {
		// 检查分片数据是否已到达
		reply.Err = ShardNotArrived
	}
	kv.mu.Unlock()
	// 如果分片不属于该组或数据未到达，直接返回错误
	if reply.Err == ErrWrongGroup || reply.Err == ShardNotArrived {
		return
	}
	// 构造 Put 或 Append 操作的命令
	command := Op{
		OpType:   args.Op,
		ClientId: args.ClientId,
		SeqId:    args.RequestId,
		Key:      args.Key,
		Value:    args.Value,
	}
	// 发送命令并等待操作完成，设置超时时间为 AppOrPutTimeout
	reply.Err = kv.startCommand(command, AppOrPutTimeout)
	return
}


// AddShard 函数用于处理来自其他服务器的 AddShard 请求，将指定的分片数据迁移到当前服务器。
func (kv *ShardKV) AddShard(args *SendShardArg, reply *AddShardReply) {
	// 构造 AddShard 操作的命令
	command := Op{
		OpType:   AddShardType,
		ClientId: args.ClientId,
		SeqId:    args.RequestId,
		ShardId:  args.ShardId,
		Shard:    args.Shard,
		SeqMap:   args.LastAppliedRequestId,
	}
	// 发送命令并等待操作完成，设置超时时间为 AddShardsTimeout
	reply.Err = kv.startCommand(command, AddShardsTimeout)
	return
}


//------------------------------------------------------handler部分------------------------------------------------------

// upConfigHandler 用于处理最新配置的更新。
func (kv *ShardKV) upConfigHandler(op Op) {
	// 获取当前配置和更新的配置
	curConfig := kv.Config
	upConfig := op.UpConfig

	// 如果当前配置号大于等于更新配置号，则不进行处理
	if curConfig.Num >= upConfig.Num {
		return
	}

	// 遍历更新配置的所有分片
	for shard, gid := range upConfig.Shards {
		// 如果当前服务器属于更新配置的组且该分片未被分配
		if gid == kv.gid && curConfig.Shards[shard] == 0 {
			// 初始化该分片的 KvMap，并设置 ConfigNum 为更新配置的配置号
			kv.shardsPersist[shard].KvMap = make(map[string]string)
			kv.shardsPersist[shard].ConfigNum = upConfig.Num
		}
	}

	// 更新 LastConfig 为当前配置，更新 Config 为更新配置
	kv.LastConfig = curConfig
	kv.Config = upConfig
}


// addShardHandler 用于处理添加分片的操作。
func (kv *ShardKV) addShardHandler(op Op) {
	// 如果该分片已经添加，或者操作是过期的，则不进行处理
	if kv.shardsPersist[op.ShardId].KvMap != nil || op.Shard.ConfigNum < kv.Config.Num {
		return
	}
	// 复制操作中分片的数据到本地
	kv.shardsPersist[op.ShardId] = kv.cloneShard(op.Shard.ConfigNum, op.Shard.KvMap)

	// 更新客户端序列号映射表
	for clientId, seqId := range op.SeqMap {
		if r, ok := kv.SeqMap[clientId]; !ok || r < seqId {
			kv.SeqMap[clientId] = seqId
		}
	}
}


// removeShardHandler 用于处理移除分片的操作。
func (kv *ShardKV) removeShardHandler(op Op) {
	// 如果操作的序列号小于当前配置号，说明是过期的操作，不进行处理
	if op.SeqId < kv.Config.Num {
		return
	}
	// 将对应分片的数据清空，并更新配置号
	kv.shardsPersist[op.ShardId].KvMap = nil
	kv.shardsPersist[op.ShardId].ConfigNum = op.SeqId
}



// PersistSnapShot 用于生成 kvserver 的快照数据。
func (kv *ShardKV) PersistSnapShot() []byte {
	// 创建一个新的字节缓冲区
	w := new(bytes.Buffer)
	// 创建一个 labgob 编码器
	e := labgob.NewEncoder(w)
	// 使用编码器将 shardsPersist、SeqMap、maxRaftState、Config、LastConfig 编码并写入缓冲区
	err := e.Encode(kv.shardsPersist)
	err = e.Encode(kv.SeqMap)
	err = e.Encode(kv.maxRaftState)
	err = e.Encode(kv.Config)
	err = e.Encode(kv.LastConfig)
	// 检查编码过程中是否发生错误
	if err != nil {
		log.Fatalf("[%d-%d] fails to take snapshot.", kv.gid, kv.me)
	}
	// 返回生成的快照数据
	return w.Bytes()
}


// DecodeSnapShot 安装给定的快照。
func (kv *ShardKV) DecodeSnapShot(snapshot []byte) {
	// 如果快照为空或长度小于1，说明是初始状态，无需处理快照
	if snapshot == nil || len(snapshot) < 1 {
		return
	}
	// 创建一个新的字节缓冲区，并使用 labgob 解码器
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)

	// 定义变量用于存储解码后的数据
	var shardsPersist []Shard
	var SeqMap map[int64]int
	var MaxRaftState int
	var Config, LastConfig shardctrler.Config

	// 使用解码器将快照数据解码到相应的变量中
	if d.Decode(&shardsPersist) != nil || d.Decode(&SeqMap) != nil ||
		d.Decode(&MaxRaftState) != nil || d.Decode(&Config) != nil || d.Decode(&LastConfig) != nil {
		log.Fatalf("[Server(%v)] Failed to decode snapshot!!!", kv.me)
	} else {
		// 更新 kvserver 的状态为解码后的数据
		kv.shardsPersist = shardsPersist
		kv.SeqMap = SeqMap
		kv.maxRaftState = MaxRaftState
		kv.Config = Config
		kv.LastConfig = LastConfig
	}
}



// Kill the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// ifDuplicate 根据给定的客户端ID和序列号判断是否是一个重复的请求
func (kv *ShardKV) ifDuplicate(clientId int64, seqId int) bool {
	lastSeqId, exist := kv.SeqMap[clientId]
	if !exist {
		return false
	}
	return seqId <= lastSeqId
}

// getWaitCh 获取或创建给定索引的等待通道
func (kv *ShardKV) getWaitCh(index int) chan OpReply {
	ch, exist := kv.waitChMap[index]
	if !exist {
		kv.waitChMap[index] = make(chan OpReply, 1)
		ch = kv.waitChMap[index]
	}
	return ch
}

// allSent 判断是否所有切片已经发送给其他组
func (kv *ShardKV) allSent() bool {
	for shard, gid := range kv.LastConfig.Shards {
		// 如果当前配置中分片中的信息不匹配，且持久化中的配置号更小，说明还未发送
		if gid == kv.gid && kv.Config.Shards[shard] != kv.gid && kv.shardsPersist[shard].ConfigNum < kv.Config.Num {
			return false
		}
	}
	return true
}

// allReceived 判断是否所有切片已经收到
func (kv *ShardKV) allReceived() bool {
	for shard, gid := range kv.LastConfig.Shards {
		// 判断切片是否都收到了
		if gid != kv.gid && kv.Config.Shards[shard] == kv.gid && kv.shardsPersist[shard].ConfigNum < kv.Config.Num {
			return false
		}
	}
	return true
}

// startCommand 向 Raft 发送命令并等待执行结果
func (kv *ShardKV) startCommand(command Op, timeoutPeriod time.Duration) Err {
	kv.mu.Lock()
	index, _, isLeader := kv.rf.Start(command)
	if !isLeader {
		kv.mu.Unlock()
		return ErrWrongLeader
	}

	ch := kv.getWaitCh(index)
	kv.mu.Unlock()

	timer := time.NewTicker(timeoutPeriod)
	defer timer.Stop()

	select {
	case re := <-ch:
		kv.mu.Lock()
		delete(kv.waitChMap, index)
		if re.SeqId != command.SeqId || re.ClientId != command.ClientId {
			// 如果索引对应的命令不匹配，说明数据不一致
			kv.mu.Unlock()
			return ErrInconsistentData
		}
		kv.mu.Unlock()
		return re.Err

	case <-timer.C:
		return ErrOverTime
	}
}

// cloneShard 根据给定的配置号和键值映射创建一个新的 Shard
func (kv *ShardKV) cloneShard(ConfigNum int, KvMap map[string]string) Shard {
	migrateShard := Shard{
		KvMap:     make(map[string]string),
		ConfigNum: ConfigNum,
	}

	for k, v := range KvMap {
		migrateShard.KvMap[k] = v
	}

	return migrateShard
}




// StartServer me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxRaftState bytes, in order to allow Raft to garbage-collect its
// log. if maxRaftState is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass masters[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// UpConfig.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use masters[]
// and make_end() to send RPCs to the group owning a specific Shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxRaftState = maxraftstate
	kv.makeEnd = make_end
	kv.gid = gid
	kv.masters = masters

	// Your initialization code here.

	kv.shardsPersist = make([]Shard, shardctrler.NShards)

	kv.SeqMap = make(map[int64]int)

	// Use something like this to talk to the shardctrler:
	// kv.mck = shardctrler.MakeClerk(kv.masters)
	kv.sck = shardctrler.MakeClerk(kv.masters)
	kv.waitChMap = make(map[int]chan OpReply)
	snapshot := persister.ReadSnapshot()
	if len(snapshot) > 0 {
		kv.DecodeSnapShot(snapshot)
	}
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	go kv.applyMsgHandlerLoop()
	go kv.ConfigDetectedLoop()

	return kv
}