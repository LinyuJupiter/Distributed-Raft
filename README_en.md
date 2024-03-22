# Distributed Raft

<p align="center">
  <a href="./README_en.md">English</a> |
  <a href="./README.md">简体中文</a>
</p>


This project is a fault-tolerant distributed key-value storage system implemented in Go, based on the project framework from the <a href="https://mit-public-courses-cn-translatio.gitbook.io/mit6-824/lecture-06-raft1">MIT 6.824</a> distributed systems course.

## Features

- Implemented Raft Leader election, heartbeats, log replication, consistency, snapshot mechanism, and log compression based on the Raft-Extended paper.
- Designed client and server components, improved functionalities like insertion, deletion, retrieval, and modification, combining database operations with the Raft algorithm.
- Designed fault-tolerant solutions, completed fault tests for node disconnection, RPC packet loss, network partition, etc.
- Conducted high-concurrency tests to ensure the system can handle a large number of concurrent requests within a specified time frame.

## Installation

To run this project, you need to install Go. Then clone this repository:

```bash
git clone https://github.com/LinYujupiter/Distributed-Raft.git
cd Distributed-Raft
```

## Running

After installing all dependencies, you can test the functionality with the following command, replacing DIR with the folder name where you want to test the functionality:

```bash
cd lab_6824-master
cd DIR
```

Once inside the folder, you can test the functionality with the following command:

```bash
go test
```

## Development

- **Go**: Used for developing this program.
- **Raft**: The algorithm foundation of this program.

## Contribution

We welcome contributions in any form, whether it's proposing new features, improving code, or reporting issues. Please make sure to follow best practices and code style guidelines.