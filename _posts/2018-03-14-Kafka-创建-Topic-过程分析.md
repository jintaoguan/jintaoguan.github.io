---
layout:     post
title:      Kafka 创建 Topic 过程分析
subtitle:   Kafka 工作流程（1）
date:       2018-03-14
author:     Jintao
header-img: img/post-bg-universe.jpg
catalog: true
tags:
    - kafka
---

## Topic 简介
之间讨论了很多 Kafka 中组件的设计与实现，在这一篇文章中，我们换一个方式，讨论一下在 Kafka 中创建一个 topic 的工作流程以及相关组件的行为。首先我们介绍一下 topic 的概念。什么是 `topic` 呢？topic 是 Kafka 中的一个消息队列的标识，也可以认为是消息队列的一个 id，用于区分不同的消息队列，一个 topic 由多个 `partition` 组成，这些 partition 是通常是分布在不同的多台 Broker 上的。为 topic 设置多个 partition 可以使得单个 topic 能够接收更高的读写，因为对同一个 topic 的读写将会被分布到多台机器上。为了保证数据的可靠性，一个 partition 又会设置为多个副本 `replica`，通常会设置 2 个副本或 3 个副本。这样如果一台 broker 宕机或者下线，则另外一个 replica 就会作为 leader 接收读写，提高了可用性。

![](/img/post-img/2018-03-14/kafka-topic-replica.png)

如上图所示，这个一个名为 "topic" 的 topic，它由三个 partition 组成，`partition 0`，`partition 1` 和 `partition 2`。每个 partition 都有两个副本，假设 Kafka 集群有三台 broker（`replica 0_1` 代表 partition 0 的第一个副本），我们可以看到每个 partition 的 replica 都会分布在不同的 broker 上。在设置副本时，副本数是必须大于集群的 broker 数的，副本只有设置在不同的机器上才有作用。

## 如何创建 Topic
创建 topic 有两种方式：
 - 通过 `kafka-topics.sh` 创建一个 topic，可以设置相应的副本数让 Server 端自动进行 replica 分配，也可以直接指定手动 replica 的分配。
 - Server 端如果 `auto.create.topics.enable` 设置为 true 时，那么当 Producer 向一个不存在的 topic 发送数据时，该 topic 同样会被创建出来，副本数默认是1。

我们先来看一下通过 kafka-topics.sh 创建 topic。在 Kafka 的安装目录下，通过下面这条命令可以创建一个 partition 为3，replica 为 2 的名为 `test` 的 topic。
~~~bash
./bin/kafka-topics.sh --create --topic test --zookeeper XXXX --partitions 3 --replication-factor 2
~~~

`kafka-topics.sh` 实际上是调用 `kafka.admin.TopicCommand` 的方法来创建 topic，其实现如下：

~~~scala
  // 创建 topic 的方法

  def createTopic(zkUtils: ZkUtils, opts: TopicCommandOptions) {
    val topic = opts.options.valueOf(opts.topicOpt)
    val configs = parseTopicConfigsToBeAdded(opts)
    val ifNotExists = opts.options.has(opts.ifNotExistsOpt)
    if (Topic.hasCollisionChars(topic))
      println("WARNING: Due to limitations in metric names, topics with a period ('.') or underscore ('_') could collide. To avoid issues it is best to use either, but not both.")
    try {
      if (opts.options.has(opts.replicaAssignmentOpt)) {
        // 如果指定了 partition 各个 replica 的分布, 那么将 partition replicas 的结果验证之后直接更新到 zk 上,

        // 验证的 replicas 的代码是在 parseReplicaAssignment 中实现的

        val assignment = parseReplicaAssignment(opts.options.valueOf(opts.replicaAssignmentOpt))
        AdminUtils.createOrUpdateTopicPartitionAssignmentPathInZK(zkUtils, topic, assignment, configs, update = false)
      } else {
        // 如果没有指定 parittion replicas 分配的话，将会调用 AdminUtils.createTopic 方法创建 topic

        CommandLineUtils.checkRequiredArgs(opts.parser, opts.options, opts.partitionsOpt, opts.replicationFactorOpt)
        // partition 个数

        val partitions = opts.options.valueOf(opts.partitionsOpt).intValue
        // replica 个数

        val replicas = opts.options.valueOf(opts.replicationFactorOpt).intValue
        // 是否使用机架感知, 默认使用机架感知模式

        val rackAwareMode = if (opts.options.has(opts.disableRackAware)) RackAwareMode.Disabled
                            else RackAwareMode.Enforced
        AdminUtils.createTopic(zkUtils, topic, partitions, replicas, configs, rackAwareMode)
      }
      println("Created topic \"%s\".".format(topic))
    } catch  {
      case e: TopicExistsException => if (!ifNotExists) throw e
    }
  }
~~~

如果指定了 partition 各个 replica 的分布，那么将 partition replicas 的结果验证之后直接更新到 zookeeper 上，验证 replicas 的代码是在 `parseReplicaAssignment()` 方法中实现的，如下所示。它验证的标准是：
 - 不允许相同的 broker 包含多个属于相同 partition 的 replica
 - 所有的 partition 的 replica 的数量必须相同

~~~scala
  // 验证用户自定义的 replicas 分布是否合法

  // 返回一个 Map[Int, List[Int]] 表示的是 Map[PartitionId, List[BrokerId]]

  // key 是 PartitionId, value 是每个 replica 所在的 BrokerId (不允许出现重复)

  def parseReplicaAssignment(replicaAssignmentList: String): Map[Int, List[Int]] = {
    // 取得每个 partition 的 replicas 列表

    val partitionList = replicaAssignmentList.split(",")
    val ret = new mutable.HashMap[Int, List[Int]]()
    // 对于每个 partition

    for (i <- 0 until partitionList.size) {
      // 由 ":" 分割开的 broker id, 表示每个 replica 所在的 broker

      val brokerList = partitionList(i).split(":").map(s => s.trim().toInt)
      val duplicateBrokers = CoreUtils.duplicates(brokerList)
      if (duplicateBrokers.nonEmpty)
        // 不允许相同的 broker 包含多个属于相同 partition 的 replica

        throw new AdminCommandFailedException("Partition replica lists may not contain duplicate entries: %s".format(duplicateBrokers.mkString(",")))
      ret.put(i, brokerList.toList)
      if (ret(i).size != ret(0).size)
        // 所有的 partition 必须包含相同的 replica 数量

        throw new AdminOperationException("Partition " + i + " has different replication factor: " + brokerList)
    }
    ret.toMap
  }
~~~

上一步的 `parseReplicaAssignment()` 方法会返回一个 `Map[PartitionId, List[BrokerId]]`，key 是 PartitionId, value 是每个 replica 所在的 BrokerId 组成的 List。之后调用 `createOrUpdateTopicPartitionAssignmentPathInZK()` 方法将 replica 分布写到 zookeeper 指定的路径上。这一步中也做了验证，和上一步有一些重复。

~~~scala
def createOrUpdateTopicPartitionAssignmentPathInZK(zkUtils: ZkUtils,
                                                   topic: String,
                                                   partitionReplicaAssignment: Map[Int, Seq[Int]],
                                                   config: Properties = new Properties,
                                                   update: Boolean = false) {
  // 验证 topic 名是否包含非法字符，在 zookeeper 中是否已经存在，每个 partition 的 replica 数量是否相同等等

  validateCreateOrUpdateTopic(zkUtils, topic, partitionReplicaAssignment, config, update)

  // Configs only matter if a topic is being created. Changing configs via AlterTopic is not supported

  if (!update) {
    // write out the config if there is any, this isn't transactional with the partition assignments

    writeEntityConfig(zkUtils, getEntityConfigPath(ConfigType.Topic, topic), config)
  }

  // create the partition assignment

  // 将 partition assignment 即 replicas 分布写入 zookeeper

  writeTopicPartitionAssignment(zkUtils, topic, partitionReplicaAssignment, update)
}
~~~

`writeTopicPartitionAssignment()` 这一个方法用来向 zookeeper 写入 replica 分布的数据。

~~~scala
private def writeTopicPartitionAssignment(zkUtils: ZkUtils, topic: String, replicaAssignment: Map[Int, Seq[Int]], update: Boolean) {
  try {
    // 获取该 topic 的 zk 路径，为 “/brokers/topics/{topic_name}"

    val zkPath = getTopicPath(topic)
    // 将 replica 分布的数据转化成 json 结构，并加上 "version: 1" 这个属性

    val jsonPartitionData = zkUtils.replicaAssignmentZkData(replicaAssignment.map(e => e._1.toString -> e._2))

    // 向该 topic 的 zkPath 中写入 replica 分布的 json 数据

    if (!update) {
      info("Topic creation " + jsonPartitionData.toString)
      zkUtils.createPersistentPath(zkPath, jsonPartitionData)
    } else {
      info("Topic update " + jsonPartitionData.toString)
      zkUtils.updatePersistentPath(zkPath, jsonPartitionData)
    }
    debug("Updated path %s with %s for replica assignment".format(zkPath, jsonPartitionData))
  } catch {
    case _: ZkNodeExistsException => throw new TopicExistsException(s"Topic '$topic' already exists.")
    case e2: Throwable => throw new AdminOperationException(e2.toString)
  }
}
~~~

创建 topic 之后，可以使用 zookeeper 的客户端看到 replica 分布的数据。在我自己的机器上查看 test 的 replica 分布，通过 `zkCli.sh` 执行 `get /brokers/topics/test`。我们可以得到 replica 分布为 `{"version":1,"partitions":{"0":[0]}}`，表示这个 topic（`test`）只有一个 partition，partition 的 id 是 `0`，其只有一个 replica，这个 replica 存放在 broker id 为 `0` 的 broker 上。

~~~
[zk: localhost:2181(CONNECTED) 0] get /brokers/topics/test
{"version":1,"partitions":{"0":[0]}}
cZxid = 0x6e3
ctime = Wed Nov 22 01:30:19 PST 2017
mZxid = 0x6e3
mtime = Wed Nov 22 01:30:19 PST 2017
pZxid = 0x6e7
cversion = 1
dataVersion = 0
aclVersion = 0
ephemeralOwner = 0x0
dataLength = 36
numChildren = 1
~~~


## Replica 分布算法
在创建 topic 时，如果用户没有指定自定义的 replica 分布，将会调用 AdminUtils.createTopic 方法创建 topic。这个过程中通过调用 `AdminUtils.assignReplicasToBrokers()` 方法来获取该 topic partition 的 replicas 分配。

~~~scala
  def assignReplicasToBrokers(brokerMetadatas: Seq[BrokerMetadata],
                              nPartitions: Int,
                              replicationFactor: Int,
                              fixedStartIndex: Int = -1,
                              startPartitionId: Int = -1): Map[Int, Seq[Int]] = {
    if (nPartitions <= 0)
      throw new InvalidPartitionsException("number of partitions must be larger than 0")
    if (replicationFactor <= 0)
      throw new InvalidReplicationFactorException("replication factor must be larger than 0")
    if (replicationFactor > brokerMetadatas.size)
      throw new InvalidReplicationFactorException(s"replication factor: $replicationFactor larger than available brokers: ${brokerMetadatas.size}")
    if (brokerMetadatas.forall(_.rack.isEmpty))
      assignReplicasToBrokersRackUnaware(nPartitions, replicationFactor, brokerMetadatas.map(_.id), fixedStartIndex,
        startPartitionId)
    else {
      if (brokerMetadatas.exists(_.rack.isEmpty))
        throw new AdminOperationException("Not all brokers have rack information for replica rack aware assignment")
      assignReplicasToBrokersRackAware(nPartitions, replicationFactor, brokerMetadatas, fixedStartIndex,
        startPartitionId)
    }
  }
~~~

副本分配时，有三个原则：
 - 将副本平均分布在所有的 broker 上。
 - partition 的多个副本应该分配在不同的 broker 上。
 - 如果所有的 Broker 有机架信息的话，partition 的副本应该分配到不同的机架上。

为实现上面的目标,在没有机架感知的情况下，应该按照下面两个原则分配 replica:
 - 从 broker list 随机选择一个 broker，使用 round-robin 算法分配每个 partition 的第一个副本。
 - 对于这个 partition 的其他副本，逐渐增加 Broker.id 来选择 replica 的分配。

`assignReplicasToBrokersRackUnaware()` 则是机架不感知分配 replica 算法的实现。机架感知算法与其相似，在这里就不做分析。

~~~scala
// 机架不感知 RackUnaware 分配 replica 算法

private def assignReplicasToBrokersRackUnaware(nPartitions: Int,
                                               replicationFactor: Int,
                                               brokerList: Seq[Int],
                                               fixedStartIndex: Int,
                                               startPartitionId: Int): Map[Int, Seq[Int]] = {
  val ret = mutable.Map[Int, Seq[Int]]()
  val brokerArray = brokerList.toArray
  val startIndex = if (fixedStartIndex >= 0) fixedStartIndex else rand.nextInt(brokerArray.length)
  var currentPartitionId = math.max(0, startPartitionId)
  var nextReplicaShift = if (fixedStartIndex >= 0) fixedStartIndex else rand.nextInt(brokerArray.length)
  for (_ <- 0 until nPartitions) {
    if (currentPartitionId > 0 && (currentPartitionId % brokerArray.length == 0))
      nextReplicaShift += 1
    val firstReplicaIndex = (currentPartitionId + startIndex) % brokerArray.length
    val replicaBuffer = mutable.ArrayBuffer(brokerArray(firstReplicaIndex))
    for (j <- 0 until replicationFactor - 1)
      replicaBuffer += brokerArray(replicaIndex(firstReplicaIndex, nextReplicaShift, j, brokerArray.length))
    ret.put(currentPartitionId, replicaBuffer)
    currentPartitionId += 1
  }
  ret
}
~~~

根据以上代码，我们可以举一个例子。假设一个 Kafka 集群有 5 个节点，新建的 topic 有 10 个 partition，并且是三副本，假设最初随机选择的 startIndex 和 nextReplicaShift 节点均为 0：
 - partition 为 0 时，那第一副本在 (0+0)%5=0，第二个副本在 (0+(1+(0+0)%5)))%5=1，第三副本在 (0+(1+(0+1)%5)))%5=2；
 - partition 为 2 时，那第一副本在 (0+2)%5=2，第二个副本在 (2+(1+(0+0)%5)))%5=3，第三副本在 (2+(1+(0+1)%5)))%5=4；
 - partition 为 5 时，那第一副本在 (0+5)%5=0，第二个副本在 (0+(1+(1+0)%5)))%5=2，第三副本在 (0+(1+(1+1)%5)))%5=3（partition 数是 Broker 数一倍时，nextReplicaShift 值会增加1）
 - partition 为 8 时，那第一副本在 (0+8)%5=3，第二个副本在 (3+(1+(1+0)%5)))%5=0，第三副本在 (3+(1+(1+1)%5)))%5=1。

那么最终的 replica 的分布如下表所示：

broker-0 | broker-1 | broker-2 | broker-3 | broker-4 |
:---: | :---: | :---: | :---: | :---: | :---:
p0 | p1 | p2 | p3 | p4 | (1st replica)
p5 | p6 | p7 | p8 | p9 | (1st replica)
p4 | p0 | p1 | p2 | p3 | (2nd replica)
p8 | p9 | p5 | p6 | p7 | (2nd replica)
p3 | p4 | p0 | p1 | p2 | (3nd replica)
p7 | p8 | p9 | p5 | p6 | (3nd replica)

## Zookeeper 监听器
之前的创建 topic 的代码只会将 replica 分布的信息写入 zookeeper，但是事实上 Kafka 对于新创建的 topic 还没有任何动作。这里我们需要知道 `KafkaController` 这个概念。

在 Kafka 早期版本，对于分区和副本的状态的管理依赖于 zookeeper 的 Watcher 和队列：每一个 broker 都会在 zookeeper 注册 Watcher，所以 zookeeper 就会出现大量的 Watcher, 如果宕机的 broker 上的 partition 很多比较多，会造成多个 Watcher 触发，造成集群内大规模调整；每一个 replica 都要再次去 zookeeper 上注册监视器，当集群规模很大的时候，zookeeper 负担很重。这种设计很容易出现脑裂和羊群效应以及 zookeeper 集群过载。在新的设计中，Kafka 集群中多个 broker，有一个会被选举为 controller leader，负责管理整个集群中分区和副本的状态。如果一个 partition 的 leader 副本故障，由 controller 负责为该 partition 重新选举新的 leader 副本；当检测到 `ISR` 列表发生变化，由 controller 通知集群中所有 broker 更新其 `MetadataCache` 信息；或者增加某个 topic 分区的时候也会由 controller 管理分区的重新分配工作。每个 broker 启动的时候，都会创建 KafkaController 对象，但是集群中只能有一个 controller leader 对外提供服务，这些 broker 上的 KafkaController 都会尝试在指定的 zookeeper 路径下创建临时节点，只有第一个成功创建的节点的 KafkaController 才可以成为 controller leader，其余的都是follower。当 controller leader故障后，所有的 follower 会收到通知，再次竞争在该路径下创建节点从而选举新的 controller leader。当一个 KafkaController 成功当选为 leader 后会调用 `partitionStateMachine.registerListeners()` 注册 zookeeper 监听器，监听 `/brokers/topics/` 路径，当该路径下的数据的数据发生变化的时候（即topic 的 replica 更新到 zk 上），监控 zk 这个目录的方法 `TopicChangeListener.doHandleChildChange()` 就会被触发。该方法由 zookeeper client 触发，两个参数分别是 `parentPath` 和 `children` 表示监控的 zk 路径和该 zk 路径下的当前数据。以下是 zookeeper 监听器 `TopicChangeListener` 处理数据变化的代码：
 - 从 zookeeper 获取新增加的 topic `newTopics` 和新删除的 topic `deletedTopics`
 - 更新 KafkaController 管理的 topic 为 zookeeper 中当前保存的 topic 集合
 - 从 zookeeper 获取 newTopics 的 replica 分布，即一个 `Map[TopicAndPartition, Seq[Int]]` 对象，该对象表示的是 TopicAndPartition -> 所有 replica 所在 broker id 列表。
 - 更新 KafkaController 的 replica 分布，增加 newTopics 的 replica 分布，并删除 deletedTopics 的 replica 分布。
 - 如果有新增加的 topic，则调用 `controller.onNewTopicCreation()` 方法创建 partition 和 replica 对象。

~~~scala
    // 当一个 topic 的 replicas 更新到 zk 上后, 监控 zk 这个目录的方法会被触发 TopicChangeListener.doHandleChildChange() 方法

    def doHandleChildChange(parentPath: String, children: Seq[String]) {
      inLock(controllerContext.controllerLock) {
        if (hasStarted.get) {
          try {
            val currentChildren = {
              debug("Topic change listener fired for path %s with children %s".format(parentPath, children.mkString(",")))
              children.toSet
            }
            // 获取新增的 topic

            val newTopics = currentChildren -- controllerContext.allTopics      
            // 获取删除的 topic

            val deletedTopics = controllerContext.allTopics -- currentChildren
            // 更新当前 controller 管理的 topic 为 zk 当前的 topic

            controllerContext.allTopics = currentChildren

            // 从 zookeeper 获得新增 topic 的 replica 分配信息 Map[TopicAndPartition, Seq[Int]]

            // 表示的是 Partition -> 所有 replica 所在 broker id 列表

            val addedPartitionReplicaAssignment = zkUtils.getReplicaAssignmentForTopics(newTopics.toSeq)
            // 将 deletedTopics 的 replicas 从 controller 的缓存中删除

            // filter(condition) 方法保留 condition == true 的元素

            controllerContext.partitionReplicaAssignment = controllerContext.partitionReplicaAssignment.filter(p =>
              !deletedTopics.contains(p._1.topic))
            // 并将新增 topic 的 replicas 更新到 controller 的缓存中

            controllerContext.partitionReplicaAssignment.++=(addedPartitionReplicaAssignment)
            info("New topics: [%s], deleted topics: [%s], new partition replica assignment [%s]".format(newTopics,
              deletedTopics, addedPartitionReplicaAssignment))
            // TODO: 这里有一个疑问，为什么这里没有将 deletedTopics 相关的 partition/replica 从状态机中删除的操作？

            if (newTopics.nonEmpty)
              // 在 KafkaController 中创建 partition 和 replica 对象

              controller.onNewTopicCreation(newTopics, addedPartitionReplicaAssignment.keySet)

          } catch {
            case e: Throwable => error("Error while handling new topic", e)
          }
        }
      }
    }
~~~

KafkaController 中 `onNewTopicCreation()` 方法先对这些 topic 注册 `PartitionChangeListener` 监听器，监控 zk 路径 `/brokers/topics/{topic_name}` 下的数据变化。然后再调用 `onNewPartitionCreation()` 方法创建 partition 和 replicas 的实例对象。

~~~scala
 def onNewTopicCreation(topics: Set[String], newPartitions: Set[TopicAndPartition]) {
   info("New topic creation callback for %s".format(newPartitions.mkString(",")))
   // subscribe to partition changes

   // 对每个新建的 topic 在 partitionStateMachine 上注册 PartitionChangeListener

   topics.foreach(topic => partitionStateMachine.registerPartitionChangeListener(topic))
   onNewPartitionCreation(newPartitions)
 }
~~~

topic 创建的主要实现是在 `KafkaController.onNewPartitionCreation()` 这个方法中，当有新topic 或 partition 时，这个方法将会被调用。
 - 将 partition 从 `NonExistentPartition` 更新至 `NewPartition` 状态
 - 将 replica（属于该 partition 的多个 replica）从 `NonExistentReplica` 更新至 `NewReplica` 状态
 - 将 partition 从 `NewPartition` 更新至 `OnlinePartition` 状态
 - 将 replica（属于该 partition 的多个 replica）从 `NewReplica` 更新至 `OnlineReplica` 状态

~~~scala
 def onNewPartitionCreation(newPartitions: Set[TopicAndPartition]) {
   info("New partition creation callback for %s".format(newPartitions.mkString(",")))
   partitionStateMachine.handleStateChanges(newPartitions, NewPartition)
   replicaStateMachine.handleStateChanges(controllerContext.replicasForPartition(newPartitions), NewReplica)
   partitionStateMachine.handleStateChanges(newPartitions, OnlinePartition, offlinePartitionSelector)
   replicaStateMachine.handleStateChanges(controllerContext.replicasForPartition(newPartitions), OnlineReplica)
 }
~~~

在详细介绍这四个方法的调用之前，先简单详述一下 Partition 状态机和 Replica 状态机。

## Partition/Replica 状态机
为什么要使用状态机？状态机这种设计适用于什么场景呢？我认为，当一个对象满足以下两个个条件的时候使用状态机这种设计比较好：1）一个对象存在多种状态，并且每种状态之间可以发生转移。2）状态转移的过程中会发生行为，并且具体行为既与旧状态（状态转移之前的状态）有关，也与新状态（状态转移之后的状态）有关。完整的 Partition/Replica 状态机代码我们会在以后具体分析，现在我们只分析一下创建 Topic 过程中涉及到的 Partition/Replica 状态机代码。

![](/img/post-img/2018-03-14/partition-state-machine.png)

一个 Partition 对象有四种状态，partition 只有在 `OnlinePartition` 这个状态时，才是可用状态。
 - `NonExistentPartition`：这个 partition 不存在。
 - `NewPartition`：这个 partition 刚创建，有对应的 replicas，但还没有 leader 和 ISR
 - `OnlinePartition`：这个 partition 的 leader 已经选举出来了，处理正常的工作状态；
 - `OfflinePartition`：partition 的 leader 挂了。

![](/img/post-img/2018-03-14/replica-state-machine.png)

一个 Replica 对象有七种状态：
 - `NewReplica`： 创建新 Topic 或进行副本重新分配时，新创建的副本就处于这个状态。处于此状态的副本只能成为 Follower 副本。
 - `OnlineReplica`：副本开始正常工作时处于此状态，处于此状态的副本可以成为 Leader 副本，也可以成为 Follower 副本。
 - `OfflineReplica`：副本所在的 Broker 下线后，会转换为此状态。
 - `ReplicaDeletionStarted`：刚开始删除副本时，会先将副本转换为此状态，然后开始删除操作。
 - `ReplicaDeletionSuccessful`：副本删除成功后，副本状态会处于此状态。
 - `ReplicaDeletionIneligible`：如果副本删除操作失败，会将副本转换为此状态。
 - `NonExistentReplica`：副本被成功删除后，最终会转换为此状态。

创建 Topic 的过程中，Partition/Replica 状态机的工作流程如下列代码所示：

`第一步`：Partition 状态机更新 partition 状态： `NonExistentPartition` -> `NewPartition`。实际上只是更新状态，并打印相关的日志，并没有什么行为。

~~~scala
  // 从 partition 状态机中获取 partition 的当前状态

  // 如果没有该 partition，则认为该 partition 处于 NonExistentPartition 状态

  val currState = partitionState.getOrElseUpdate(topicAndPartition, NonExistentPartition)
  // ... 省略

  case NewPartition =>
    // pre: partition did not exist before this

    // 确认允许该 partition 当前处于合法的转移前状态集合，即 List(NonExistentPartition)

    assertValidPreviousStates(topicAndPartition, List(NonExistentPartition), NewPartition)
    // 这一步仅仅是更新状态机中该 partition 的状态到 NewPartition 状态

    partitionState.put(topicAndPartition, NewPartition)
    val assignedReplicas = controllerContext.partitionReplicaAssignment(topicAndPartition).mkString(",")
    stateChangeLogger.trace("Controller %d epoch %d changed partition %s state from %s to %s with assigned replicas %s"
                              .format(controllerId, controller.epoch, topicAndPartition, currState, targetState,
                                      assignedReplicas))
  // ... 省略
~~~

`第二步`：Replica 状态机更新 replica 状态： `NonExistentReplica` -> `NewReplica`。

~~~scala
  // 从 Replica 状态机中获取 replica 的当前状态

  // 如果没有该 replica，则认为该 replica 处于 NonExistentReplica 状态

  val currState = replicaState.getOrElseUpdate(partitionAndReplica, NonExistentReplica)
  // ... 省略

  case NewReplica =>
    // 确认允许该 replica 当前处于合法的转移前状态集合，即 List(NonExistentReplica)

    assertValidPreviousStates(partitionAndReplica, List(NonExistentReplica), targetState)  
    // start replica as a follower to the current leader for its partition

    val leaderIsrAndControllerEpochOpt = ReplicationUtils.getLeaderIsrAndEpochForPartition(zkUtils, topic, partition)
    leaderIsrAndControllerEpochOpt match {
      case Some(leaderIsrAndControllerEpoch) =>
        // 这个状态的 Replica 不能作为 leader

        if(leaderIsrAndControllerEpoch.leaderAndIsr.leader == replicaId)
          throw new StateChangeFailedException("Replica %d for partition %s cannot be moved to NewReplica"
            .format(replicaId, topicAndPartition) + "state as it is being requested to become leader")
        // 向所有 replicaId 发送 LeaderAndIsr 请求,这个方法同时也会向所有的 broker 发送 updateMeta 请求

        brokerRequestBatch.addLeaderAndIsrRequestForBrokers(List(replicaId),
                                                            topic, partition, leaderIsrAndControllerEpoch,
                                                            replicaAssignment)
      case None => // new leader request will be sent to this replica when one gets elected
    }
    // ... 省略

~~~

`第三步`：Partition 状态机更新 partition 状态： `NewPartition` -> `OnlinePartition`。
 - 初始化 leader 和 ISR，replicas 中的第一个 replica 将作为 leader，所有 replica 作为 ISR，并把 leader 和 ISR 信息更新到 zookeeper。
 - 发送 LeaderAndIsr 请求给所有的 replica，发送 UpdateMetadata 给所有 Broker。

~~~scala
  // post: partition has been assigned replicas
  case OnlinePartition =>
    // 确认允许该 replica 当前处于合法的转移前状态集合，即 List(NewPartition, OnlinePartition, OfflinePartition)

    assertValidPreviousStates(topicAndPartition, List(NewPartition, OnlinePartition, OfflinePartition), OnlinePartition)
    partitionState(topicAndPartition) match {
      case NewPartition =>
        // initialize leader and isr path for new partition

        // 状态转移为：NewPartition -> OnlinePartition，为新建的 partition 初始化 leader 和 ISR

        initializeLeaderAndIsrForPartition(topicAndPartition)
      case OfflinePartition =>
          electLeaderForPartition(topic, partition, leaderSelector)
      case OnlinePartition => // invoked when the leader needs to be re-elected
          electLeaderForPartition(topic, partition, leaderSelector)
      case _ => // should never come here since illegal previous states are checked above
    }
  // ... 省略

~~~

实际的操作是在 `initializeLeaderAndIsrForPartition()` 方法中完成，这个方法是当 partition 对象的状态由 NewPartition 变为 OnlinePartition 时触发的，用来初始化该 partition 的 leader 和 ISR。做了下面这几件事情：
 - 获取 partition 的 replica 分布，获取存活的 broker 列表，取交集。
 - 如果交集为 0，则当前没有存活的 replica，抛出异常
 - 如果交集不是 0，则选取第一个 replica 作为 leader，所有的 Replica 作为 ISR。
 - 将 leader 和 ISR 的信息写到 zookeeper 上，路径为 `/brokers/topics/[topic_name]/partitions/[partition_id]/state`
 - 调用 `brokerRequestBatch.addLeaderAndIsrRequestForBrokers` 向所有 replicaId（即 brokerId）发送 `LeaderAndIsrRequest` 请求，它也会向所有的 broker 发送 `UpdateMetadataRequest` 请求。

~~~scala
  private def initializeLeaderAndIsrForPartition(topicAndPartition: TopicAndPartition) {
    // 获得该 partition 的所有 replica 所在的 brokerId

    val replicaAssignment = controllerContext.partitionReplicaAssignment(topicAndPartition)
    // 获取当前可用的 brokerId

    val liveAssignedReplicas = replicaAssignment.filter(r => controllerContext.liveBrokerIds.contains(r))
    liveAssignedReplicas.size match {
      case 0 =>
        // 如果 AR 中没有存活的副本集，抛出状态转换失败的异常

        val failMsg = ("encountered error during state change of partition %s from New to Online, assigned replicas are [%s], " +
                       "live brokers are [%s]. No assigned replica is alive.")
                         .format(topicAndPartition, replicaAssignment.mkString(","), controllerContext.liveBrokerIds)
        stateChangeLogger.error("Controller %d epoch %d ".format(controllerId, controller.epoch) + failMsg)
        throw new StateChangeFailedException(failMsg)
      case _ =>
        debug("Live assigned replicas for partition %s are: [%s]".format(topicAndPartition, liveAssignedReplicas))
        // make the first replica in the list of assigned replicas, the leader

        // 让第一个 replica (brokerId) 成为 leader

        val leader = liveAssignedReplicas.head
        // 创建LeaderIsrAndControllerEpoch对象, 让所有的 replica (brokerId) 成为 ISR

        val leaderIsrAndControllerEpoch = new LeaderIsrAndControllerEpoch(new LeaderAndIsr(leader, liveAssignedReplicas.toList),
          controller.epoch)
        debug("Initializing leader and isr for partition %s to %s".format(topicAndPartition, leaderIsrAndControllerEpoch))
        try {
          // 将 leader 和 ISR 更新到 zookeeper 上

          // 根据 leaderIsrAndControllerEpoch 信息在 zookeeper 创建 /brokers/topics/[topic_name]/partitions/[partition_id]/state

          zkUtils.createPersistentPath(
            getTopicPartitionLeaderAndIsrPath(topicAndPartition.topic, topicAndPartition.partition),
            zkUtils.leaderAndIsrZkData(leaderIsrAndControllerEpoch.leaderAndIsr, controller.epoch))
          // NOTE: the above write can fail only if the current controller lost its zk session and the new controller

          // took over and initialized this partition. This can happen if the current controller went into a long

          // GC pause

          // 更新 ControllerContext 中 partition 对应的 leader 和 ISR

          controllerContext.partitionLeadershipInfo.put(topicAndPartition, leaderIsrAndControllerEpoch)
          // 添加 LeaderAndIsr 请求到队列，将该 partition 的 leader 和 ISR 信息发送给所有的 replica 所在的 broker

          // 也会将 UpdateMetadata 请求加入队列，向所有的 broker 发送

          brokerRequestBatch.addLeaderAndIsrRequestForBrokers(liveAssignedReplicas, topicAndPartition.topic,
            topicAndPartition.partition, leaderIsrAndControllerEpoch, replicaAssignment)
        } catch {
          case _: ZkNodeExistsException =>
            // read the controller epoch

            val leaderIsrAndEpoch = ReplicationUtils.getLeaderIsrAndEpochForPartition(zkUtils, topicAndPartition.topic,
              topicAndPartition.partition).get
            val failMsg = ("encountered error while changing partition %s's state from New to Online since LeaderAndIsr path already " +
                           "exists with value %s and controller epoch %d")
                             .format(topicAndPartition, leaderIsrAndEpoch.leaderAndIsr.toString(), leaderIsrAndEpoch.controllerEpoch)
            stateChangeLogger.error("Controller %d epoch %d ".format(controllerId, controller.epoch) + failMsg)
            throw new StateChangeFailedException(failMsg)
        }
    }
  }
~~~

为什么 KafkaController 要向所有的 replica 发送 `LeaderAndIsrRequest` 请求呢？我想是因为各个 replica 可以根据 LeaderAndIsrRequest 知道自己是 leader 还是 follower，然后根据自己的角色选择是否向 replica leader 那里发送 fetch 来同步数据。那为什么 KafkaController 要向所有的 broker 发送 `LeaderAndIsrRequest` 请求呢？因为当 topic 创建后，metadata 会发生变化，而每台 broker 都需要保存整个 cluster 全部 topic 的 metadata。具体的过程我们以后继续分析，这里设一个疑问？


`第四步`：Replica 状态机更新 replica 状态： `NewReplica` -> `OnlineReplica`。

~~~scala
  case OnlineReplica =>
    assertValidPreviousStates(partitionAndReplica,
      List(NewReplica, OnlineReplica, OfflineReplica, ReplicaDeletionIneligible), targetState)
    replicaState(partitionAndReplica) match {
      case NewReplica =>
        // 状态转移为：NewReplica -> OnlineReplica

        // add this replica to the assigned replicas list for its partition

        val currentAssignedReplicas = controllerContext.partitionReplicaAssignment(topicAndPartition)
        if(!currentAssignedReplicas.contains(replicaId))
          controllerContext.partitionReplicaAssignment.put(topicAndPartition, currentAssignedReplicas :+ replicaId)
        stateChangeLogger.trace("Controller %d epoch %d changed state of replica %d for partition %s from %s to %s"
                                  .format(controllerId, controller.epoch, replicaId, topicAndPartition, currState,
                                          targetState))
  // ... 省略

~~~

## 总结
至此，我们基本了解了 Kafka 创建 Topic 的过程，其中涉及了客户端，ZooKeeper 和 KafkaController。
 - 客户端判断创建的 Topic 是否合法，并且自动生成 replica 分布，并将其写至 ZooKeeper。
 - KafkaController 启动时会生成监听器监听相关路径，当 topic 列表发生变化，触发监听器。
 - 监听器触发后会更新 Partition/Replica 状态机中 partition 和 replica 的状态，并调用相关方法，并通知其他 broker。

参考文章：
* [Kafka 源码解析之 topic 创建过程](http://matt33.com/2017/07/21/kafka-topic-create/)
