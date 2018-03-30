---
layout:     post
title:      uReplicator - Uber 开发的 Kafka Replicator
subtitle:   Uber Engineering Blog 翻译文章
date:       2018-03-28
author:     Jintao
header-img: img/post-bg-2015.jpg
catalog: true
tags:
    - kafka
    - uber
---
# Uber 的 Kafka 架构
在 Uber，我们使用 `Apache Kafka` 作为数据总线来连接整个生态系统中的各个部分。我们收集系统或者应用程序的日志和 Driver 和 Rider App 的发来的数据。然后我们通过 Kafka 将这些数据提供给不同的下游消费者。Kafka 中的数据既提供给实时处理的 pipeline 也会提供给批处理的 pipeline。实时处理的数据用于计算业务指标，调试，警报和数据表。批处理的数据用于向 `Apache Hadoop` 和 `HP Vertica` 写数据的 ETL。

![](/img/post-img/2018-03-28/kafka-uber.png)

在这片文章中，我们会讲解 [uReplicator, Uber’s open source solution](https://github.com/uber/uReplicator) 复制 Kafka 数据的方法。这个系统拓展了 Kafka 的 `MirrorMaker` 的设计，关注高可用，保证无数据丢失和更简易的运维。从 2015 年 11 月开始，uReplicator 已经成为了 Uber 多数据中心基础设施的重要组成部分。

## 什么是 MirrorMaker
什么是 MirrorMaker？我们为什么需要 MirrorMaker ？因为 Uber 对 Kafka 的大规模使用，我们在多个数据中心拥有多个 Kafka 集群。在很多种用例中，我们需要站在全局的视角上看待数据。举一个例子，为了计算关于 trip 的业务指标，我们需要从各个数据中心获取数据并在一个地方进行分析。为了达到这个目的，我们原先使用了开源的 [Apache MirrorMaker](https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=27846330) 进行跨数据中心的数据复制。如下图所示。

![](/img/post-img/2018-03-28/kafka-uber-mirror.png)

MirrorMaker（Kafka 0.8.2）本身非常简单。它使用 Kafka Consumer 从源数据中心获取数据，再使用 Kafka Producer 发给目标数据中心。

## MirrorMaker 的局限
尽管在开始的时候 MirrorMaker 已经足够了，但是很快我们就遇到了 scalability 的问题。因为 topic 数量和数据量（字节数每秒）的增加，我们开始观察到数据传输的延迟以及数据的完全丢失，这导致了生产故障，也降低了数据质量。以下就是现在 Uber 的特殊用例中 MirrorMaker（Kafka 0.8.2）出现的主要问题：
  - 昂贵的 rebalance。就像我们上面说过的，每个 MirrorMaker worker 都使用一个 consumer。这些 consumer 时常会经历 rebalance 这一过程。在 rebalance 时，它们会彼此协商来决定谁拥有哪个 topic-partition（通过 [Apache Zookeeper](https://zookeeper.apache.org/) 来完成）。这个过程会花费大量时间，我们观察到在某些极端情况下，这个过程花费了 5 - 10 分钟。这是一个很严重的故障，因为它违反了我们的端到端的延时的服务保证。另外，consumer 会在 32 次 rebalance 尝试失败后直接放弃，这就会使这个 consumer 永远地卡住。不幸的是，我们观察到这种现象发生了很多次。在每一次 rebalance 尝试之后，我们都可以观察到下面的流量变化：![](/img/post-img/2018-03-28/kafka-uber-rebalance.png) 在 rebalance 过程中，因为这个卡顿， MirrorMaker 会积累大量的数据需要追赶，这又导致了之后的一次流量暴增。随后，所有下游消费者都会发生生产故障，也增加了端到端的延迟。
  - 添加 topic 的困难。在 Uber，我们显式指定了 topic 白名单来控制复制的数据量。白名单是完全静态的，每当我们向其中添加新的 topic 时都需要重启整个 MirrorMaker 集群。重启操作是非常昂贵的，因为它会强制 consumer 做 rebalance。
  - 可能的数据丢失。旧的 MirrorMaker 存在一个 automatic offset commit 的问题（似乎在新版本中已经修复），会导致数据丢失。
  - Metadata 同步问题。我们也遇到了一个运维的问题，即更新配置的方式。为了从白名单添加和删除 topic，我们列出

## 我们为什么开发 uReplicator
我们通过下面这个方式来解决上面提到的这些问题：

**A.分解成多个 MirrorMaker 集群。** 上面列出的大多数问题都源于 consumer 的 rebalance 过程。一个降低其影响的方法就是限制 MirrorMaker 集群需要复制的 topic-partition 的数量。这样，我们就会有多个 MirrorMaker 集群，每个集群负责复制一部分 topic-partition。

好处：
 - 添加新 topic 很容易，只需要创建一个新的集群。
 - MirrorMaker 集群的重启很快。

坏处：
 - 运维很痛苦，需要部署和维护多个 MirrorMaker 集群。

**B.使用 Apache Samza 来复制数据。** 因为这些问题都和 consumer 相关，一个解决办法是使用 Kafka 的 [SimpleConsumer](https://cwiki.apache.org/confluence/display/KAFKA/0.8.0+SimpleConsumer+Example) 再加上 leader 选举。[Apache Samza](http://samza.apache.org/) 是一个流处理框架，它静态地将 partition 分配给 worker。我们可以简单地使用一个 Samza 作业来复制聚合数据到目标数据中心。

好处：
 - 它非常稳定可靠。
 - 维护很容易。我们可以在一次作业中复制很多 topic。
 - 作业重启对复制的流量的影响很小。

坏处：
 - Samza 作业非常静态，我们需要重启作业来增加或删除 topic。
 - 我们需要重启 Samza 作业来增加 worker。
 - topic 扩展需要显式地处理。

**C.使用一个基于 Apache Helix 的 Kafka consumer。** 最终，我们决定使用一个基于 Apache Helix 的 Kafka consumer。我们使用 Apache Helix 来分配 partition 给 worker，然后每个 worker 使用 SimpleConsumer 来复制数据。

好处：
 - 增加或删除 topic 非常简单。
 - 从 MirrorMaker 集群增加或删除结点很容易。
 - 我们不需要为了维护而重启集群（但是升级需要重启集群）。
 - 非常可靠稳定。

坏处：
 - 它会带来对于 Apache Helix 的依赖（因为 Helix 本身非常稳定而且我们可以使用一个 Helix 集群为多个 MirrorMaker 集群服务，所以这不是一个大问题）。

## uReplicator 架构

![](/img/post-img/2018-03-28/kafka-ureplicator.jpg)

uReplicator 的多个组件在可靠性和稳定性方面工作方式也不相同。

- `Helix uReplicator controller`，也就是集群的节点，会负责下列这些工作：
  - 将 topic-partition 分配给各个 worker 进程。
  - 处理 topic/partition 的添加删除。
  - 处理 uReplicator worker 的添加删除。
  - 检测节点失败，并且重新分配这个节点负责的 topic-partition。

这个 controller 使用 Zookeeper 来完成这些任务。它还提供了一套简单的 REST API 来添加/删除/修改需要复制的 topic。

- `uReplicator worker`，类似于 Kafka mirroring 特性中的 worker 进程，它会从源数据中心复制一组特定的 topic-partition 到目数据中心。uReplicator controller 负责分配复制任务而不是 rebalance。并且，我们使用了一个简化版的 consumer 叫做 **DynamicKafkaConsumer** 而不是 Kafka 的 high-level consumer。
- `Helix agent` 每一个 uReplicator worker 拥有一个 Helix agent，在添加删除 topic-partition 时，Helix agent 会受到通知。相应的，它会通知 DynamicKafkaConsumer 来添加删除 topic-partition。
- `DynamicKafkaConsumer` 是修改过的 `high-level consumer`，存在于每一个 uReplicator worker 进程中。它去除了 rebalance 部分，并且增加了一种运行时添加删除 topic-partition 的机制。

举一个例子，假如我们想向一个现有的 uReplicator 集群添加一个新的 topic，事件顺序是这样的：
 - Kafka 管理员使用下面的命令向 controller 添加这个新的 topic。
 ~~~bash
 $ curl -X POST http://localhost:9000/topics/testTopic
 ~~~
 - uReplicator controller 找到了这个 testTopic 拥有的 partition 数量，然后将它的 topic-partition 映射到活动的 worker 上去。然后它会更新 Zookeeper 的 metadata 来表现出这个映射。
 - 每一个对应的 Helix agent 会收到添加 topic-partition 的通知。相应的，这个 agent 会触发 **DynamicKafkaConsumer** 的 **addFetcherForPartitions** 方法。
 - **DynamicKafkaConsumer** 随后会注册这些新的 partition，找到其对应的 leader broker，然后将它们添加到抓取线程，从而开始数据复制。

## 对于总体可扩展性的影响
自从在八个月之前 Uber 开始使用 uReplicator，我们再也没有看见过一次生产故障。对比之前，我们几乎每周就会有一次。

总体来说，使用 uReplicator 的好处包括：
 - 稳定性。现在 rebalance 只会在集群启动以及添加删除节点的时候发生。另外，它只会影响一部分 topic-partition 而不是之前那样会影响所有的 topic-partition。
 - 更简易地扩容。向集群添加一个新节点变容易了。因为现在 partition 分配是静态的，我们可以移动一部分 partition 到一个新节点，而其它的 topic-partition 则不受影响。
 - 更简单地运维。支持动态白名单，当增加删除扩展 topic 时，我们不需要重启集群。
 - 零数据丢失。uReplicator 保证了零数据丢失，因为它会在确保数据被持久化在目标数据中心后记录 checkpoint。


参考文章
* [uReplicator: Uber Engineering’s Robust Kafka Replicator](https://eng.uber.com/ureplicator/)




















1
