---
layout:     post
title:      KafkaController 选举过程分析
subtitle:   Kafka 工作流程（2）
date:       2018-03-22
author:     Jintao
header-img: img/post-bg-universe.jpg
catalog: true
tags:
    - kafka
---

## KafkaController 简介
在 Kafka 早期版本，对于分区和副本的状态的管理依赖于 zookeeper 的 Watcher 和队列：每一个 broker 都会在 zookeeper 注册 Watcher，所以 zookeeper 就会出现大量的 Watcher, 如果宕机的 broker 上的 partition 比较多，会造成多个 Watcher 触发，造成集群内大规模调整。每一个 replica 都要去再次 zookeeper 上注册监视器，当集群规模很大的时候，zookeeper 负担很重。这种设计很容易出现脑裂和羊群效应以及 zookeeper 集群过载。

新的版本中该变了这种设计，使用 `KafkaController`，只有 KafkaController 的 Leader 会向 zookeeper 上注册 Watcher，其他 broker 几乎不用监听 zookeeper 的状态变化。Kafka 集群中多个 broker，有一个会被选举为 controller leader，负责管理整个集群中分区和副本的状态，比如 partition 的 leader 副本故障，由 controller 负责为该 partition 重新选举新的 leader 副本；当检测到 ISR 列表发生变化，有 controller 通知集群中所有 broker 更新其 `MetadataCache` 信息；或者增加某个 topic 分区的时候也会由 controller 管理分区的重新分配工作。

我们知道 zookeeper 集群中也有选举机制，是通过 `Paxos` 算法，通过不同节点向其他节点发送信息来投票选举出 leader，但是 Kafka 的 leader 的选举就没有这么复杂了。Kafka 的 Leader 选举是通过在 zookeeper 上创建 `/controller` 临时节点来实现 leader 选举，并在该节点中写入当前 broker 的信息：

~~~bash
{“version”:1,”brokerid”:0,”timestamp”:”1512018424988”}
~~~

利用 zookeeper 的强一致性特性，一个节点只能被一个客户端创建成功，创建成功的 broker 即为 leader，即先到先得原则。leader 也就是集群中的 controller，负责集群中所有大小事务。当 leader 和 zookeeper 失去连接时，临时节点会删除，而其他 broker 会监听该节点的变化，当节点删除时，其他 broker 会收到事件通知，重新发起 leader 选举。

## KafkaController 启动
我们已经知道，每个 broker 都有一个 KafkaController 实例，但是只有一个通过选举成为了 leader。我们从 `KafkaServer.startup()` 方法开始看起，其中这一步创建 KafkaController 实例。

~~~scala
  def startup() {
    // ... 省略

    kafkaController = new KafkaController(config, zkUtils, brokerState, time, metrics, threadNamePrefix)
    kafkaController.startup()
    // ... 省略

  }
~~~

KafkaController 的构造方法以及 `startup()` 方法。值得关注的是 `ZookeeperLeaderElector` 的构造函数中有两个函数参数，分别是 `onControllerFailover` 和 `onControllerResignation`。onControllerFailover() 方法是当前 broker 新当选为 leader 后的回调函数。onControllerResignation() 方法是当前 broker 从 leader 卸任后的回调函数。我们在稍后对它们进行分析。并注册了一个 SessionExpirationListener 用于对 zookeeper session 连接状态进行监控。

~~~scala
class KafkaController(val config: KafkaConfig, zkUtils: ZkUtils, val brokerState: BrokerState, time: Time, metrics: Metrics, threadNamePrefix: Option[String] = None) extends Logging with KafkaMetricsGroup {
  // ... 省略

  private val controllerElector = new ZookeeperLeaderElector(controllerContext, ZkUtils.ControllerPath,
    onControllerFailover, onControllerResignation, config.brokerId, time)
  // ... 省略

  def startup() = {
    inLock(controllerContext.controllerLock) {
      info("Controller starting up")
      registerSessionExpirationListener()
      isRunning = true
      // 在调用 controllerElector.startup() 后集群就开始通过 zookeeper 选举 leader 了

      controllerElector.startup
      info("Controller startup complete")
    }
  }

  private def registerSessionExpirationListener() = {
    zkUtils.zkClient.subscribeStateChanges(new SessionExpirationListener())
  }
}
~~~


## SessionExpirationListener 分析
`SessionExpirationListener` 用于对 zookeeper session 连接状态进行监控。当 broker 和 zookeeper 重新建立连接后，它的 `handleNewSession()` 方法会被调用。先判断当前 zookeeper 的 `/controller` 路径中存储的 controller (即当前的 leader)是否是自己。如果不是，则先关闭相关模块，然后尝试重新竞选。

~~~scala
class SessionExpirationListener() extends IZkStateListener with Logging {
  this.logIdent = "[SessionExpirationListener on " + config.brokerId + "], "

  def handleStateChanged(state: KeeperState) {
    // do nothing, since zkclient will do reconnect for us.

  }

  // 当 zookeeper 的 session 过期，一个新的 session 创建后，handleNewSession() 会被触发

  @throws[Exception]
  def handleNewSession() {
    info("ZK expired; shut down all controller components and try to re-elect")
    if (controllerElector.getControllerID() != config.brokerId) {
      onControllerResignation()
      inLock(controllerContext.controllerLock) {
        controllerElector.elect
      }
    } else {
      info("ZK expired, but the current controller id %d is the same as this broker id, skip re-elect".format(config.brokerId))
    }
  }

  def handleSessionEstablishmentError(error: Throwable): Unit = {
    //no-op handleSessionEstablishmentError in KafkaHealthCheck should handle this error in its handleSessionEstablishmentError

  }
}
~~~


## ZookeeperLeaderElector 分析
KafkaController 的 leader 选举过程比较简单，几乎全程由 `ZookeeperLeaderElector` 这个组件进行控制。在 KafkaController 类中，有这样一个成员 `controllerElector`，它是一个 ZookeeperLeaderElector 对象。ZookeeperLeaderElector 类实现 leader 选举的功能，但是它并不负责处理 broker 和 zookeeper 的会话超时（连接超时）的情况，而是认为调用者应该在会话恢复（连接重新建立）时进行重新选举。

我们继续看一下 `ZookeeperLeaderElector` 的构造方法，该构造方法中使用了下面这几个参数：
 - `controllerContext`：一个 ControllerContext 对象，记载了 broker，topic，partition，replica 分布等信息，内部有一个 `ControllerChannelManager` 对象，可以将其看做是 zookeeper 的缓存。
 - `electionPath`：一个 String 常量，表示 zookeeper 上存储 controller 信息的路径，值为 `/controller`。
 - `onBecomingLeader`：一个 () => Unit 方法，表示当前 broker 新当选为 leader 后的回调函数。
 - `onResigningAsLeader`：一个 () => Unit 方法，表示当前 broker 从 leader 卸任后的回调函数。
 - `brokerId`：一个 Int 变量，表示当前 broker 的 id。
 - `time`: 一个 Time 对象，表示当前时刻。

~~~scala
class ZookeeperLeaderElector(controllerContext: ControllerContext,
                             electionPath: String,
                             onBecomingLeader: () => Unit,     // 被选举为 leader 的回调函数

                             onResigningAsLeader: () => Unit,  // 从 leader 卸任的回调函数

                             brokerId: Int,
                             time: Time)
  extends LeaderElector with Logging {
  var leaderId = -1
  // create the election path in ZK, if one does not exist

  val index = electionPath.lastIndexOf("/")
  if (index > 0)
    controllerContext.zkUtils.makeSurePersistentPathExists(electionPath.substring(0, index))
  // 创建 LeaderChangeListener 监听器

  val leaderChangeListener = new LeaderChangeListener

  def startup {
    inLock(controllerContext.controllerLock) {
      // 添加 /controller 节点的 IZkDataListener 监听器

      controllerContext.zkUtils.zkClient.subscribeDataChanges(electionPath, leaderChangeListener)
      // 进行选举

      elect
    }
  }
}
~~~

`ZookeeperLeaderElector.startup()` 方法中会调用 `ZookeeperLeaderElector.elect()` 方法选举 leader。

~~~scala
// 读取并解析 zookeeper 中选举路径的 controller 数据，返回当前 controller 的 brokerId

def getControllerID(): Int = {
  controllerContext.zkUtils.readDataMaybeNull(electionPath)._1 match {
     case Some(controller) => KafkaController.parseControllerId(controller)
     case None => -1
  }
}

// 进行选举

def elect: Boolean = {
  val timestamp = time.milliseconds.toString
  val electString = Json.encode(Map("version" -> 1, "brokerid" -> brokerId, "timestamp" -> timestamp))

  // 从 zookeeper 获取当前 controller 的 brokerId

  leaderId = getControllerID
  // 如果读取 leader 数据成功, 那么集群已经存在了一个 leader, 则终止选举过程, 返回 amILeader()

  if(leaderId != -1) {
     debug("Broker %d has been elected as leader, so stopping the election process.".format(leaderId))
     return amILeader
  }

  // 如果读取 leader 数据失败, 则当前没有 leader, 尝试竞选 leader

  try {
    // 在 zookeeper 选举路径上尝试创建临时节点

    val zkCheckedEphemeral = new ZKCheckedEphemeral(electionPath,
                                                    electString,
                                                    controllerContext.zkUtils.zkConnection.getZookeeper,
                                                    JaasUtils.isZkSecurityEnabled())
    zkCheckedEphemeral.create()
    info(brokerId + " successfully elected as leader")
    leaderId = brokerId
    // 调用选举成功的回调函数

    onBecomingLeader()
  } catch {
    case _: ZkNodeExistsException =>
      // 如果已经有别的 broker 注册了临时结点，则获取 controller 的 brokerId

      leaderId = getControllerID

      if (leaderId != -1)
        debug("Broker %d was elected as leader instead of broker %d".format(leaderId, brokerId))
      else
        warn("A leader has been elected but just resigned, this will result in another round of election")

    // 当在发送数据到 zookeeper 过程中出现 Throwable 异常时，会调用 resign() 方法

    case e2: Throwable =>
      error("Error while electing or becoming leader on broker %d".format(brokerId), e2)
      resign()
  }
  amILeader
}

def close = {
  leaderId = -1
}

// 查看当前缓存的 leader 是否是自己

def amILeader : Boolean = leaderId == brokerId

// 删除 zookeeper 的 leader 信息

// 但是它会触发其他所有的 broker 上 LeaderChangeListener 的 handleDataDeleted(), 从而发生重新选举

def resign() = {
  leaderId = -1
  controllerContext.zkUtils.deletePath(electionPath)
}
~~~

有下面几种情况会调用 elect 方法：
 - broker启动时，第一次调用
 - 上一次创建节点成功，但是可能在等 zookeeper 响应的时候，连接中断，`resign()` 方法中删除 `/controller` 节点后，触发了 `LeaderChangeListener.handleDataDeleted()` 方法。
 - 上一次创建节点未成功，但是可能在等 zookeeper 响应的时候，连接中断，而再次进入 elect 方法时，已有别的 broker 创建 controller 节点成功，成为了 leader。
 - 上一次创建节点成功，但是 `onBecomingLeader()` 方法抛出了异常，而再次进入，所以 elect 方法中先获取 /controller 节点信息，判断是否已经存在，然后再尝试选举 leader。



## LeaderChangeListener 分析
在之前 ZookeeperLeaderElector 的启动方法中注册了一个监听器，其中 `electionPath` 就是 zookeeper 上记录 controller 数据的路径 `/controller`。`LeaderChangeListener` 监听器的作用是监控整个 Kafka 集群的 leader 变化。LeaderChangeListener 监听器会对 zookeeper 路径 /controller 进行监控。当该路径上有数据发生更新时，`handleDataChange()` 方法就会被触发。而如果该路径上的数据被删除，则 `handleDataDeleted()` 方法就会被触发。

当 handleDataChange() 方法被触发时，表示当前 Kafka 集群的 KafkaController leader 发生了变化：
 - 首先判断在 leader 变化之前自己是不是 leader。
 - 如果自己之前不是 leader 而现在也不是 leader，那么和自己没关系，什么也不做。
 - 如果自己之前是 leader 而现在不是 leader 了，需要卸任，调用 onResigningAsLeader() 方法。
 - **如果自己之前不是 leader 而现在是 leader，也什么也不做。为什么？没明白。**

当 handleDataChange() 方法被触发时，表示当前 Kafka 集群的 KafkaController leader 消失了：**（什么情况会导致 leader 消失？）**
 - 首先判断在 leader 消失之前自己是不是 leader。
 - 如果自己之前是 leader，现在没有 leader，那么首先卸任，调用 onResigningAsLeader() 方法，再调用 elect() 重新参加 leader 竞选。

~~~scala
class LeaderChangeListener extends IZkDataListener with Logging {
  // 当 dataPath 路径上的数据更改时，handleDataChange() 方法会被触发

  @throws(classOf[Exception])
  def handleDataChange(dataPath: String, data: Object) {
    inLock(controllerContext.controllerLock) {
      val amILeaderBeforeDataChange = amILeader
      leaderId = KafkaController.parseControllerId(data.toString)
      info("New leader is %d".format(leaderId))
      // The old leader needs to resign leadership if it is no longer the leader

      if (amILeaderBeforeDataChange && !amILeader)
        // 如果之前是 leader，而现在不是 leader，则调用卸任 leader 的回调方法

        onResigningAsLeader()
    }
  }

  // 当 dataPath 路径上的数据被删除时，handleDataDeleted() 方法会被触发

  @throws(classOf[Exception])
  def handleDataDeleted(dataPath: String) {
    inLock(controllerContext.controllerLock) {
      debug("%s leader change listener fired for path %s to handle data deleted: trying to elect as a leader"
        .format(brokerId, dataPath))
      if(amILeader)
        // 如果数据删除之前是 leader，则调用卸任 leader 的回调方法

        onResigningAsLeader()
      // 重新尝试选举成 leader

      elect
    }
  }
}
~~~

## 当选/卸任 leader 的回调方法
我们之前一直都在分析 KafkaController 选举 leader 的过程，但是我们还没有看过当选/卸任 leader 后触发的回调函数，也就是 `onControllerFailover()` 和 `onControllerResignation()`。

我们先来分析当选 leader 后调用的回调函数 onControllerFailover()：

~~~scala
  def onControllerFailover() {
    if(isRunning) {
      info("Broker %d starting become controller state transition".format(config.brokerId))
      // 从 zookeeper 读取 controller epoch/version 数据, 存放进 controller 的 controllerContext 对象

      readControllerEpochFromZookeeper()
      // 更新 epoch/version 数据并存进 zookeeper

      incrementControllerEpoch(zkUtils.zkClient)

      // before reading source of truth from zookeeper, register the listeners to get broker/topic callbacks

      // 注册各类 zookeeper 监听器

      registerReassignedPartitionsListener()
      registerIsrChangeNotificationListener()
      registerPreferredReplicaElectionListener()
      // 注册监听器 TopicChangeListener 和 DeleteTopicsListener, 对 topic 变化进行监听

      partitionStateMachine.registerListeners()
      // 注册监听器 BrokerChangeListener, 对 broker 变化进行监听

      replicaStateMachine.registerListeners()

      // 读取 zookeeper 初始化该 KafkaController 对象的 ControllerContext 对象

      // 这时候该 KafkaController 已经是 leader 了

      initializeControllerContext()

      // We need to send UpdateMetadataRequest after the controller context is initialized and before the state machines

      // are started. The is because brokers need to receive the list of live brokers from UpdateMetadataRequest before

      // they can process the LeaderAndIsrRequests that are generated by replicaStateMachine.startup() and

      // partitionStateMachine.startup().

      sendUpdateMetadataRequest(controllerContext.liveOrShuttingDownBrokerIds.toSeq)

      // 启动 partition 状态机和 replica 状态机，用于管理 Kafka 集群状态

      replicaStateMachine.startup()
      partitionStateMachine.startup()

      // register the partition change listeners for all existing topics on failover

      // 对当前的每一个 topic 都注册监听器 PartitionChangeListener 监控 partition 变化

      controllerContext.allTopics.foreach(topic => partitionStateMachine.registerPartitionChangeListener(topic))
      info("Broker %d is ready to serve as the new controller with epoch %d".format(config.brokerId, epoch))
      maybeTriggerPartitionReassignment()
      maybeTriggerPreferredReplicaElection()
      if (config.autoLeaderRebalanceEnable) {
        info("starting the partition rebalance scheduler")
        autoRebalanceScheduler.startup()
        autoRebalanceScheduler.schedule("partition-rebalance-thread", checkAndTriggerPartitionRebalance,
          5, config.leaderImbalanceCheckIntervalSeconds.toLong, TimeUnit.SECONDS)
      }
      // 启动 topic 删除管理器

      deleteTopicManager.start()
    }
    else
      info("Controller has been shut down, aborting startup/failover")
  }
~~~

再来看一下卸任 leader 后调用的回调函数 onControllerResignation()：

~~~scala
  def onControllerResignation() {
    debug("Controller resigning, broker id %d".format(config.brokerId))
    // de-register listeners

    deregisterIsrChangeNotificationListener()
    deregisterReassignedPartitionsListener()
    deregisterPreferredReplicaElectionListener()

    // shutdown delete topic manager

    if (deleteTopicManager != null)
      deleteTopicManager.shutdown()

    // shutdown leader rebalance scheduler

    if (config.autoLeaderRebalanceEnable)
      autoRebalanceScheduler.shutdown()

    inLock(controllerContext.controllerLock) {
      // de-register partition ISR listener for on-going partition reassignment task

      deregisterReassignedPartitionsIsrChangeListeners()
      // shutdown partition state machine

      partitionStateMachine.shutdown()
      // shutdown replica state machine

      replicaStateMachine.shutdown()
      // shutdown controller channel manager

      if(controllerContext.controllerChannelManager != null) {
        controllerContext.controllerChannelManager.shutdown()
        controllerContext.controllerChannelManager = null
      }
      // reset controller context

      controllerContext.epoch=0
      controllerContext.epochZkVersion=0
      brokerState.newState(RunningAsBroker)

      info("Broker %d resigned as the controller".format(config.brokerId))
    }
  }
~~~
## 总结
可以看到，KafkaController leader 选举的过程比较复杂，需要对多种情况进行处理。其大致过程是这样：
 - 每个 broker 启动时，会创建 KafkaController 对象并启动它。启动过程中创建了 ZookeeperLeaderElector 对象并启动它。ZookeeperLeaderElector 启动过程中注册监听器 LeaderChangeListener 监控 zookeeper 的 /controller 路径，之后立刻参加 leader 竞选。
   - **一个 broker 启动之后，会立刻参加 leader 竞选。**
 - KafkaController 启动过程中注册了监听器 SessionExpirationListener 监控 zookeeper session 的连接状态。
   - **每当 zookeeper 的 session 断开重连后，如果当前 leader 不是自己，则先卸任 leader，然后参加竞选。**
 - ZookeeperLeaderElector 启动过程中注册监听器 LeaderChangeListener 监控 leader 变化。
   - **每当 leader 发生变化，如果之前是 leader，而现在不是 leader，则卸任。**
   - **每当 leader 消失，如果之前是 leader，则卸任，并参加竞选。**

关于竞选成功和卸任：
 - 如果竞选 leader 成功，KafkaController 会进行下列操作：
   - 读取 zookeeper 上之前 leader 的 epoch/version 信息存入 ControllerContext。
   - 更新为自己的 epoch/version 信息再写回 zookeeper。
   - 注册各类监听器。
   - 注册 TopicChangeListener 和 DeleteTopicsListener 监听器。
   - 注册 BrokerChangeListener 监听器。
   - 初始化 ControllerContext 对象。
   - 向 Kafka 集群中所有 broker 发送 UpdateMetadataRequest 请求。
   - 启动 partition 状态机和 replica 状态机。
   - 启动 AutoRebalanceScheduler (optional)。
   - 启动 TopicDeletionManager。

 - 从 leader 卸任，KafkaController 会进行下列操作：
   - 卸载各类监听器。
   - 关闭 TopicDeletionManager。
   - 关闭 AutoRebalanceScheduler (optional)。
   - 关闭 ControllerChannelManager。
   - 关闭 partition 状态机和 replica 状态机。
   - 清空自己的 controllerContext.epoch 和 controllerContext.epochZkVersion 信息。
   - 更新自己的状态为 RunningAsBroker。
