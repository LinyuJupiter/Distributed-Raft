# Distributed Raft

<p align="center">
  <a href="./README_en.md">English</a> |
  <a href="./README.md">简体中文</a>
</p>


本项目是一个基于 <a href="https://mit-public-courses-cn-translatio.gitbook.io/mit6-824/lecture-06-raft1">MIT 6.824</a>分布式课程中的项目框架，利用 Go 语言实现的容错的分布式键值（key-value）存储系统。

## 特性

- 查阅 Raft-Extended 论文，完成 Raft Leader 选举和心跳、日志复制与一致性、快照机制与日志压缩等功能；
- 分别设计客户端和服务端，完善增删查改等功能，实现数据库和 Raft 算法的结合；
- 设计容错方案，完成节点断网、RPC 丢包、网络分区等故障测试；
- 完成高并发测试，系统可实现在规定时间内处理大量的并发请求。

## 安装

要运行此项目，您需要安装 go。然后克隆此仓库：

```bash
git clone https://github.com/LinYujupiter/Distributed-Raft.git
cd Distributed-Raft
```

## 运行

在安装了所有依赖之后，您可以通过以下命令测试功能，其中DIR需要改成你想测试的功能所在文件夹名：

```bash
cd lab_6824-master
cd DIR
```

在进入文件夹后，您可以通过以下命令测试功能：

```bash
go test
```

## 开发

- **Go**: 用于开发本程序。
- **Raft**: 本程序的算法基础。

## 贡献

我们欢迎任何形式的贡献，无论是新功能的提议、代码改进还是问题报告。请确保遵循最佳实践和代码风格指南。
