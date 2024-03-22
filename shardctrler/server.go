package shardctrler

import (
	"6.824/raft"
	"time"
)
import "6.824/labrpc"
import "sync"
import "6.824/labgob"

// 常量
const (
	JoinType  = "join"   // 加入服务器操作
	LeaveType = "leave"  // 离开服务器操作
	MoveType  = "move"   // 移动分片操作
	QueryType = "query"  // 查询操作

	// 超时时间常量（单位：毫秒）
	JoinOverTime  = 100
	LeaveOverTime = 100
	MoveOverTime  = 100
	QueryOverTime = 100

	// InvalidGid 所有分片应分配给 GID 为零的组（无效 GID）
	InvalidGid = 0
)

// ShardCtrler 是分片控制器的结构体
type ShardCtrler struct {
	mu        sync.Mutex        // 互斥锁
	me        int                // 服务器编号
	rf        *raft.Raft         // Raft 实例
	applyCh   chan raft.ApplyMsg // 用于接收 Raft 的 ApplyMsg
	// Your data here.
	configs   []Config           // 存储配置的数组，按配置编号索引
	seqMap    map[int64]int      // 用于重复性检测的映射，记录每个客户端最近一次的序列号
	waitChMap map[int]chan Op    // 存储等待通道的映射，根据 Raft 日志的索引找到对应的等待通道
}

// Op 是分片控制器操作的结构体
type Op struct {
	// Your data here.
	// 操作相关的数据字段
	OpType      string          // 操作类型
	ClientId    int64           // 客户端编号
	SeqId       int             // 客户端请求序列号
	QueryNum    int             // 查询操作的编号
	JoinServers map[int][]string // 加入服务器操作的参数，组编号和服务器列表的映射
	LeaveGids   []int           // 离开服务器操作的参数，离开的组编号列表
	MoveShard   int             // 移动分片操作的参数，待移动的分片编号
	MoveGid     int             // 移动分片操作的参数，目标组的编号
}



//The Join RPC is used by an administrator to add new replica groups. Its argument is a set of mappings from unique,
//non-zero replica group identifiers (GIDs) to lists of server names.
// Join参数就是为一个组，一个组对应的就是gid -> lists of server names.
// Join 处理客户端加入新服务器的请求
func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	// 检查当前节点是否为领导者
	_, ifLeader := sc.rf.GetState()
	if !ifLeader {
		reply.Err = ErrWrongLeader
		return
	}
	// 封装加入新服务器的操作并传递到底层Raft模块进行复制
	op := Op{OpType: JoinType, SeqId: args.SeqId, ClientId: args.ClientId, JoinServers: args.Servers}
	lastIndex, _, _ := sc.rf.Start(op)
	// 获取或创建等待通道，并在函数退出时删除通道
	ch := sc.getWaitCh(lastIndex)
	defer func() {
		sc.mu.Lock()
		delete(sc.waitChMap, lastIndex)
		sc.mu.Unlock()
	}()
	// 设置超时计时器
	timer := time.NewTicker(JoinOverTime * time.Millisecond)
	defer timer.Stop()
	select {
	case replyOp := <-ch:
		// 从通道接收回复Op，验证其与请求Op的一致性
		if op.ClientId != replyOp.ClientId || op.SeqId != replyOp.SeqId {
			reply.Err = ErrWrongLeader
		} else {
			// 回复正确的操作结果
			reply.Err = OK
			return
		}
	case <-timer.C:
		// 超时情况下回复错误信息
		reply.Err = ErrWrongLeader
	}
}


// Leave 处理客户端离开GID的请求
func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	// 检查当前节点是否为领导者
	_, ifLeader := sc.rf.GetState()
	if !ifLeader {
		reply.Err = ErrWrongLeader
		return
	}
	// 封装离开GID的操作并传递到底层Raft模块进行复制
	op := Op{OpType: LeaveType, SeqId: args.SeqId, ClientId: args.ClientId, LeaveGids: args.GIDs}
	lastIndex, _, _ := sc.rf.Start(op)

	// 获取或创建等待通道，并在函数退出时删除通道
	ch := sc.getWaitCh(lastIndex)
	defer func() {
		sc.mu.Lock()
		delete(sc.waitChMap, lastIndex)
		sc.mu.Unlock()
	}()
	// 设置超时计时器
	timer := time.NewTicker(LeaveOverTime * time.Millisecond)
	defer timer.Stop()
	select {
	case replyOp := <-ch:
		// 从通道接收回复Op，验证其与请求Op的一致性
		if op.ClientId != replyOp.ClientId || op.SeqId != replyOp.SeqId {
			reply.Err = ErrWrongLeader
		} else {
			// 回复正确的操作结果
			reply.Err = OK
			return
		}
	case <-timer.C:
		// 超时情况下回复错误信息
		reply.Err = ErrWrongLeader
	}
}


// Move 处理客户端移动Shard的请求
func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	// 检查当前节点是否为领导者
	_, ifLeader := sc.rf.GetState()
	if !ifLeader {
		reply.Err = ErrWrongLeader
		return
	}
	// 封装移动Shard的操作并传递到底层Raft模块进行复制
	op := Op{OpType: MoveType, SeqId: args.SeqId, ClientId: args.ClientId, MoveShard: args.Shard, MoveGid: args.GID}
	lastIndex, _, _ := sc.rf.Start(op)
	// 获取或创建等待通道，并在函数退出时删除通道
	ch := sc.getWaitCh(lastIndex)
	defer func() {
		sc.mu.Lock()
		delete(sc.waitChMap, lastIndex)
		sc.mu.Unlock()
	}()
	// 设置超时计时器
	timer := time.NewTicker(MoveOverTime * time.Millisecond)
	defer timer.Stop()
	select {
	case replyOp := <-ch:
		// 从通道接收回复Op，验证其与请求Op的一致性
		if op.ClientId != replyOp.ClientId || op.SeqId != replyOp.SeqId {
			reply.Err = ErrWrongLeader
		} else {
			// 回复正确的操作结果
			reply.Err = OK
			return
		}
	case <-timer.C:
		// 超时情况下回复错误信息
		reply.Err = ErrWrongLeader
	}
}


// The shardctrler replies with the configuration that has that number. If the number is -1 or bigger than the biggest
// known configuration number, the shardctrler should reply with the latest configuration. The result of Query(-1) should
// reflect every Join, Leave, or Move RPC that the shardctrler finished handling before it received the Query(-1) RPC.
// Query 处理客户端查询请求
func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	// 检查当前节点是否为领导者
	_, ifLeader := sc.rf.GetState()
	if !ifLeader {
		reply.Err = ErrWrongLeader
		return
	}
	// 封装查询操作并传递到底层Raft模块进行复制
	op := Op{OpType: QueryType, SeqId: args.SeqId, ClientId: args.ClientId, QueryNum: args.Num}
	lastIndex, _, _ := sc.rf.Start(op)
	// 获取或创建等待通道，并在函数退出时删除通道
	ch := sc.getWaitCh(lastIndex)
	defer func() {
		sc.mu.Lock()
		delete(sc.waitChMap, lastIndex)
		sc.mu.Unlock()
	}()
	// 设置超时计时器
	timer := time.NewTicker(QueryOverTime * time.Millisecond)
	defer timer.Stop()
	select {
	case replyOp := <-ch:
		// 从通道接收回复Op，验证其与请求Op的一致性
		if op.ClientId != replyOp.ClientId || op.SeqId != replyOp.SeqId {
			reply.Err = ErrWrongLeader
		} else {
			// 回复正确的查询结果
			sc.mu.Lock()
			reply.Err = OK
			sc.seqMap[op.ClientId] = op.SeqId
			// 根据查询的配置版本号回复相应的配置信息
			if op.QueryNum == -1 || op.QueryNum >= len(sc.configs) {
				reply.Config = sc.configs[len(sc.configs)-1]
			} else {
				reply.Config = sc.configs[op.QueryNum]
			}
			sc.mu.Unlock()
		}
	case <-timer.C:
		// 超时情况下回复错误信息
		reply.Err = ErrWrongLeader
	}
}


// applyMsgHandlerLoop 处理 applyCh 发送过来的 ApplyMsg
func (sc *ShardCtrler) applyMsgHandlerLoop() {
	for {
		select {
		case msg := <-sc.applyCh:
			// 处理正常的Command
			if msg.CommandValid {
				index := msg.CommandIndex
				op := msg.Command.(Op)
				// 判断是否是重复的请求
				if !sc.ifDuplicate(op.ClientId, op.SeqId) {
					sc.mu.Lock()
					switch op.OpType {
					case JoinType:
						// 处理加入新服务器的操作
						sc.seqMap[op.ClientId] = op.SeqId
						sc.configs = append(sc.configs, *sc.JoinHandler(op.JoinServers))
					case LeaveType:
						// 处理离开服务器的操作
						sc.seqMap[op.ClientId] = op.SeqId
						sc.configs = append(sc.configs, *sc.LeaveHandler(op.LeaveGids))
					case MoveType:
						// 处理移动分片的操作
						sc.seqMap[op.ClientId] = op.SeqId
						sc.configs = append(sc.configs, *sc.MoveHandler(op.MoveGid, op.MoveShard))
					}
					sc.mu.Unlock()
				}
				// 将返回的 Op 写入对应的等待通道 waitCh
				sc.getWaitCh(index) <- op
			}
		}
	}
}



// The shardctrler should react by creating a new configuration that includes the new replica groups. The new
//configuration should divide the shards as evenly as possible among the full set of groups, and should move as few
//shards as possible to achieve that goal. The shardctrler should allow re-use of a GID if it's not part of the
//current configuration (i.e. a GID should be allowed to Join, then Leave, then Join again).
// JoinHandler处理加入新组的操作，并返回更新后的配置。
func (sc *ShardCtrler) JoinHandler(servers map[int][]string) *Config {
	// 取出最后一个config将分组加进去
	lastConfig := sc.configs[len(sc.configs)-1]
	newGroups := make(map[int][]string)
	for gid, serverList := range lastConfig.Groups {
		newGroups[gid] = serverList
	}
	for gid, serverLists := range servers {
		newGroups[gid] = serverLists
	}

	// GroupMap: groupId -> shards
	// 记录每个分组有几个分片(group -> shards可以一对多，也因此需要负载均衡，而一个分片只能对应一个分组）
	GroupMap := make(map[int]int)
	for gid := range newGroups {
		GroupMap[gid] = 0
	}

	// 记录每个分组存了多少分片
	for _, gid := range lastConfig.Shards {
		if gid != 0 {
			GroupMap[gid]++
		}
	}

	// 都没存自然不需要负载均衡,初始化阶段
	if len(GroupMap) == 0 {
		return &Config{
			Num:    len(sc.configs),
			Shards: [10]int{},
			Groups: newGroups,
		}
	}

	// 需要负载均衡的情况
	return &Config{
		Num:    len(sc.configs),
		Shards: sc.loadBalance(GroupMap, lastConfig.Shards),
		Groups: newGroups,
	}
}


// LeaveHandler 处理需要离开的组
// The shardctrler should create a new configuration that does not include those groups, and that assigns those groups'
// shards to the remaining groups. The new configuration should divide the shards as evenly as possible among the groups,
// and should move as few shards as possible to achieve that goal.
func (sc *ShardCtrler) LeaveHandler(gids []int) *Config {
	// 用map模拟set来记录需要离开的GIDs
	leaveMap := make(map[int]bool)
	for _, gid := range gids {
		leaveMap[gid] = true
	}
	// 获取当前最新的配置
	lastConfig := sc.configs[len(sc.configs)-1]
	// 创建新的分组映射，将离开的GIDs从当前配置中删除
	newGroups := make(map[int][]string)
	// 取出最新配置的groups组进行填充
	for gid, serverList := range lastConfig.Groups {
		newGroups[gid] = serverList
	}
	// 删除对应的gid的值
	for _, leaveGid := range gids {
		delete(newGroups, leaveGid)
	}
	// GroupMap: groupId -> shards
	// 记录每个分组有几个分片（group -> shards可以一对多，也因此需要负载均衡，而一个分片只能对应一个分组）
	GroupMap := make(map[int]int)
	newShard := lastConfig.Shards
	// 对GroupMap进行初始化
	for gid := range newGroups {
		if !leaveMap[gid] {
			GroupMap[gid] = 0
		}
	}
	// 遍历当前分片配置，更新GroupMap和新的分片配置
	for shard, gid := range lastConfig.Shards {
		if gid != 0 {
			// 如果这个组在leaveMap中，则将对应分片数置为0
			if leaveMap[gid] {
				newShard[shard] = 0
			} else {
				GroupMap[gid]++
			}
		}
	}
	// 如果没有分组，直接返回一个初始化的配置
	if len(GroupMap) == 0 {
		return &Config{
			Num:    len(sc.configs),
			Shards: [10]int{},
			Groups: newGroups,
		}
	}
	// 需要负载均衡的情况，返回一个更新后的配置
	return &Config{
		Num:    len(sc.configs),
		Shards: sc.loadBalance(GroupMap, newShard),
		Groups: newGroups,
	}
}

// MoveHandler 为指定的分片分配指定的组
// The shardctrler should create a new configuration in which the shard is assigned to the group. The purpose of Move is
// to allow us to test your software. A Join or Leave following a Move will likely un-do the Move, since Join and Leave
// re-balance.
func (sc *ShardCtrler) MoveHandler(gid int, shard int) *Config {
	// 获取当前最新的配置
	lastConfig := sc.configs[len(sc.configs)-1]
	// 复制当前配置信息生成新的配置
	newConfig := Config{
		Num:    len(sc.configs),
		Shards: [10]int{},
		Groups: map[int][]string{},
	}
	// 复制分片信息
	for shards, gids := range lastConfig.Shards {
		newConfig.Shards[shards] = gids
	}
	// 移动分片到目标分组
	newConfig.Shards[shard] = gid
	// 复制分组信息
	for gids, servers := range lastConfig.Groups {
		newConfig.Groups[gids] = servers
	}
	// 返回包含移动操作的新配置
	return &newConfig
}


// Kill
// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
}

// Raft needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// ifDuplicate 检查给定客户端ID和序列号是否代表一个重复的请求
func (sc *ShardCtrler) ifDuplicate(clientId int64, seqId int) bool {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	// 检查客户端ID是否存在于映射中
	lastSeqId, exist := sc.seqMap[clientId]
	if !exist {
		return false
	}
	// 如果给定序列号小于等于上一次处理的序列号，表示请求重复
	return seqId <= lastSeqId
}

// getWaitCh 获取或创建给定索引的等待通道
func (sc *ShardCtrler) getWaitCh(index int) chan Op {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	// 检查索引对应的等待通道是否存在，如果不存在则创建
	ch, exist := sc.waitChMap[index]
	if !exist {
		sc.waitChMap[index] = make(chan Op, 1)
		ch = sc.waitChMap[index]
	}
	return ch
}


/// 负载均衡函数
// GroupMap : gid -> servers[]
// lastShards : shard -> gid
func (sc *ShardCtrler) loadBalance(GroupMap map[int]int, lastShards [NShards]int) [NShards]int {
	// 计算平均每个分组应该分配的分片数量
	length := len(GroupMap)
	ave := NShards / length
	remainder := NShards % length
	// 按照分片数量进行排序，以便后续负载均衡操作
	sortGids := sortGroupShard(GroupMap)

	// 先处理负载过多的分组，释放一些分片
	for i := 0; i < length; i++ {
		target := ave
		// 判断这个分组是否需要额外的分片，因为不能完全均分，前面的分组应该多分一些
		if !moreAllocations(length, remainder, i) {
			target = ave + 1
		}
		// 如果当前分组的负载超过了目标值，则进行释放
		if GroupMap[sortGids[i]] > target {
			overLoadGid := sortGids[i]
			changeNum := GroupMap[overLoadGid] - target
			for shard, gid := range lastShards {
				if changeNum <= 0 {
					break
				}
				if gid == overLoadGid {
					lastShards[shard] = InvalidGid
					changeNum--
				}
			}
			GroupMap[overLoadGid] = target
		}
	}

	// 处理负载较少的分组，分配一些多余的分片
	for i := 0; i < length; i++ {
		target := ave
		if !moreAllocations(length, remainder, i) {
			target = ave + 1
		}
		// 如果当前分组的负载低于目标值，则进行分配
		if GroupMap[sortGids[i]] < target {
			freeGid := sortGids[i]
			changeNum := target - GroupMap[freeGid]
			for shard, gid := range lastShards {
				if changeNum <= 0 {
					break
				}
				if gid == InvalidGid {
					lastShards[shard] = freeGid
					changeNum--
				}
			}
			GroupMap[freeGid] = target
		}
	}

	return lastShards
}

// 根据分片数量进行排序
// GroupMap : groupId -> shard nums
func sortGroupShard(GroupMap map[int]int) []int {
	length := len(GroupMap)
	gidSlice := make([]int, 0, length)
	// 将 map 转换成有序的 slice
	for gid, _ := range GroupMap {
		gidSlice = append(gidSlice, gid)
	}
	// 对分片数量进行排序，负载高的排在前面
	for i := 0; i < length-1; i++ {
		for j := length - 1; j > i; j-- {
			if GroupMap[gidSlice[j]] < GroupMap[gidSlice[j-1]] || (GroupMap[gidSlice[j]] == GroupMap[gidSlice[j-1]] && gidSlice[j] < gidSlice[j-1]) {
				gidSlice[j], gidSlice[j-1] = gidSlice[j-1], gidSlice[j]
			}
		}
	}
	return gidSlice
}

// 判断是否需要额外分配
func moreAllocations(length int, remainder int, i int) bool {
	// 这个目的是判断 index 是否在安排 ave+1 的前列:3、3、3、1，ave: 10/4 = 2.5 = 2，则负载均衡后应该是2+1,2+1,2,2
	if i < length-remainder {
		return true
	} else {
		return false
	}
}



// StartServer
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
//
// 初始化
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me
	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}
	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)
	// Your code here.
	// You may need initialization code here.
	sc.seqMap = make(map[int64]int)
	sc.waitChMap = make(map[int]chan Op)
	go sc.applyMsgHandlerLoop()
	return sc
}