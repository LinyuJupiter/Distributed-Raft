package kvraft

// 定义常量表示操作结果
const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
)

// Err 表示错误类型
type Err string

// Put or Append
// PutAppendArgs 包含 Put 或 Append 请求的参数
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Id int64 // 请求的唯一标识符
}

// PutAppendReply 包含 Put 或 Append 请求的响应
type PutAppendReply struct {
	Err Err // 错误类型
}

// GetArgs 包含 Get 请求的参数
type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	Id int64 // 请求的唯一标识符
}

// GetReply 包含 Get 请求的响应
type GetReply struct {
	Err   Err  // 错误类型
	Value string // 获取到的值
}
