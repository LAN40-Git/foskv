# FosKV

![C++](https://img.shields.io/badge/standard-C++23-00599C?logo=cplusplus&logoColor=white) ![Linux](https://img.shields.io/badge/platform-linux-dimgray)

FosKV 是一个异步非阻塞的 C++ 分布式kv存储系统，底层使用 **io_uring** 和 **C++协程** 驱动，具备选举、日志同步、KV存储等核心功能。



```
oooooooooooo                    oooo                    
`888'     `8                    `888                    
 888          .ooooo.   .oooo.o  888  oooo  oooo    ooo 
 888oooo8    d88' `88b d88(  "8  888 .8P'    `88.  .8'  
 888    "    888   888 `"Y88b.   888888.      `88..8'   
 888         888   888 o.  )88b  888 `88b.     `888'    
o888o        `Y8bod8P' 8""888P' o888o o888o     `8'     

```



## 核心模块

1. **kosio 异步运行时**
2. **RPC 框架**

此模块基于 **kosio搭建**，为整个分布式系统提供异步非阻塞的 RPC 通信，服务端和客户端均实现了流水线化，通过内置缓冲区来避免请求的拷贝，内置**重连、会话管理**等机制，支持异步并发请求，在**单客户端**压测下性能约 **25000QPS**

- 服务端维护一个会话管理器（共享指针管理），每个会话都会被拆分为生产者和消费者的角色，生产者负责持续读取数据，记录每一个 RPC 调用并封装为任务，消费者负责消费这些任务，进行 RPC 调用，形成 **SPSC** 模型。
- 客户端提供 `call` 方法，通过 **异步互斥锁** 保证线程安全，外部调用充当生产者，RPC 框架会将所有调用封装为任务并交给一个协程进行消费，形成 **MPSC** 模型。

3. **Raft共识算法**

此模块由**传输层（Transport）、配置层（RaftConfig）、持久层（Persister、RaftLog和StateMachine）**构成。**传输层**负责与其它节点之间的通信，**配置层**负责保存集群配置或从（json）文件中加载配置，**持久层**负责持久化日志、状态以及从磁盘中加载持久化数据。

**单客户端、3节点集群**压测下性能约 **7000QPS**。

4. **RocksDB存储**



## 部署

目前暂不支持命令行参数，可修改以下代码中的集群信息和配置文件路径部署。

```c++
#include "foskv/raft/raft_node.hpp"
#include <kosio/signal/signal.hpp>

using namespace foskv::raft;

auto process(std::unique_ptr<RaftNode>& node) -> kosio::async::Task<> {
    auto ret = co_await node->run();
    if (!ret) {
        LOG_ERROR("{}", ret.error());
    }
}

auto main_loop() -> kosio::async::Task<> {
    std::unordered_set<NodeInfo> nodes;
    nodes.emplace("node1", "127.0.0.1", 8080);
    nodes.emplace("node2", "127.0.0.1", 8081);
    nodes.emplace("node3", "127.0.0.1", 8082);
    // Node1
    auto has_save = co_await RaftConfig::save("node1/config.json", 0, "node1", nodes);
    if (!has_save) {
        LOG_ERROR("{}", has_save.error());
        co_return;
    }
    auto has_raft_node = co_await RaftNode::create("node1/config.json", "node1");
    if (!has_raft_node) {
        LOG_ERROR("{}", has_raft_node.error());
        co_return;
    }
    auto node = std::move(has_raft_node.value());
    kosio::spawn(process(node));
    co_await kosio::signal::ctrl_c();
    // Exit
    co_await node->shutdown();
}

auto main() -> int {
    SET_LOG_LEVEL(kosio::log::LogLevel::Verbose);
    kosio::runtime::CurrentThreadBuilder::default_create().block_on(main_loop());
}
```



## Todo lists

1. 批量同步日志
2. 日志补发
3. 快照
