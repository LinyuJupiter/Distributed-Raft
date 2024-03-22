package kvraft

import (
	"crypto/rand"
	"math/big"
	"strconv"

	"6.824/labrpc"
)

// Clerk 结构体用于向 KV 服务器发起客户端请求。
type Clerk struct {
	servers []*labrpc.ClientEnd // KV 服务器的 RPC 客户端数组
	leader  int                  // 记录当前 KV 服务器的领导者
	peerNum int                  // KV 服务器的数量
	name    string               // 客户端的名称
}

// nrand 生成一个伪随机的 int64 数字。
func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

// MakeClerk 创建并返回一个 Clerk 客户端。
func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.leader = -1
	ck.peerNum = len(servers)
	ck.name = strconv.FormatInt(nrand(), 10)
	return ck
}

//
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
//
// Get 从 KV 服务器获取指定 key 对应的值。
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	if ck.leader == -1 {
		ck.leader = 0
	}
	args := &GetArgs{key, nrand()}
	reply := &GetReply{}
	for {
		// 调用 KV 服务器的 Get 方法
		ok := ck.servers[ck.leader].Call("KVServer.Get", args, reply)
		// 处理 Consensus error
		if reply.Err == "Consensus error" {
			ck.leader = (ck.leader + 1) % ck.peerNum
		}
		// 处理 Not leader 错误
		if reply.Err == "Not leader" {
			ck.leader = (ck.leader + 1) % ck.peerNum
		}
		// 处理调用失败情况
		if !ok {
			ck.leader = (ck.leader + 1) % ck.peerNum
		}
		// 成功获取值，返回
		if ok && reply.Err == "" {
			return reply.Value
		}
		reply.Err = ""
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
// PutAppend 向 KV 服务器发送 Put 或 Append 请求。
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	if ck.leader == -1 {
		ck.leader = 0
	}
	args := &PutAppendArgs{key, value, op, nrand()}
	reply := &PutAppendReply{}
	for {
		// 调用 KV 服务器的 PutAppend 方法
		ok := ck.servers[ck.leader].Call("KVServer.PutAppend", args, reply)
		// 处理 Consensus error
		if reply.Err == "Consensus error" {
			ck.leader = (ck.leader + 1) % ck.peerNum
		}
		// 处理 Not leader 错误
		if reply.Err == "Not leader" {
			ck.leader = (ck.leader + 1) % ck.peerNum
		}
		// 处理调用失败情况
		if !ok {
			ck.leader = (ck.leader + 1) % ck.peerNum
		}
		// 成功发送请求，退出循环
		if ok && reply.Err == "" {
			break
		}
		reply.Err = ""
	}
}

// Put 向 KV 服务器发送 Put 请求。
func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

// Append 向 KV 服务器发送 Append 请求。
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}