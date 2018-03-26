---
layout:     post
title:      Kafka 组件 Sender 线程分析
subtitle:   Kafka 网络通信基础 (3)
date:       2018-03-25
author:     Jintao
header-img: img/post-bg-universe.jpg
catalog: true
tags:
    - kafka
---

## Sender 线程概要
在我们之前的这片文章 [Kafka 客户端 Producer 分析](https://jintaoguan.github.io/2017/12/20/Kafka-Producer-%E5%88%86%E6%9E%90/) 中，我们在分析 `KafkaProducer` 类的时候发现每当需要向 Kafka 服务器发送数据或者请求的时候，都会去调用 `sender.wakeup()` 方法：
 - `KafkaProducer.doSend()` 发送 KafkaProducer 中的数据。
 - `KafkaProducer.waitOnMetadata()` 向 Kafka 服务器请求最新的 metadata 信息。
 - `KafkaProducer.flush()` 发送 KafkaProducer 中所有的数据。

在以上三个方法中，最后一步都是通过调用 sender.wakeup() 实现数据或者请求的发送。

为什么每次调用 sender.wakeup() 方法就会自动向 Kafka 服务器发送信息呢？我们可以通过今天的这片文章明白其中的原理。

## Sender 线程运行分析
在 KafkaProducer 的构造函数中，创建了 `NetworkClient` 对象，并将其作为参数创建了 `Sender` 对象。之后将 sender 包装在一个 `KafkaThread`（KafkaThread 是对 Thread 的简单封装）中，并启动 Sender 线程。之后，进入 `Sender.run()` 方法。所以我们知道每次创建一个 KafkaProducer 对象，其实都会启动一个 Sender 线程用于向各个 broker 发送数据。

~~~java
    private KafkaProducer(ProducerConfig config, Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        try {
            // 省略...

            // 创建 NetworkClient, 它是 KafkaProducer 网络 I/O 的核心模块
            NetworkClient client = new NetworkClient(
                    new Selector(config.getLong(ProducerConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG), this.metrics, time, "producer", channelBuilder),
                    this.metadata,
                    clientId,
                    config.getInt(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION),
                    config.getLong(ProducerConfig.RECONNECT_BACKOFF_MS_CONFIG),
                    config.getInt(ProducerConfig.SEND_BUFFER_CONFIG),
                    config.getInt(ProducerConfig.RECEIVE_BUFFER_CONFIG),
                    this.requestTimeoutMs,
                    time,
                    true);
            this.sender = new Sender(client,
                    this.metadata,
                    this.accumulator,
                    config.getInt(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION) == 1,
                    config.getInt(ProducerConfig.MAX_REQUEST_SIZE_CONFIG),
                    (short) parseAcks(config.getString(ProducerConfig.ACKS_CONFIG)),
                    config.getInt(ProducerConfig.RETRIES_CONFIG),
                    this.metrics,
                    Time.SYSTEM,
                    this.requestTimeoutMs);
            String ioThreadName = "kafka-producer-network-thread" + (clientId.length() > 0 ? " | " + clientId : "");

            // 启动 Sender 对应的线程
            this.ioThread = new KafkaThread(ioThreadName, this.sender, true);
            this.ioThread.start();

            //省略...

        } catch (Throwable t) {
            // 异常处理, 省略...
        }
    }
~~~

Sender 线程被启动后，进入 `run()` 方法。变量 running 用于标志当前时刻 Sender 线程是否在运行，在 Sender 构造函数中 running 已经被设为 true，在 `initiateClose()` 方法中会被设为 false。run() 方法会不断调用另一个带参数的 run() 方法。

~~~java
public void run() {
    log.debug("Starting Kafka producer I/O thread.");

    // main loop, runs until close is called
    while (running) {
        try {
            run(time.milliseconds());
        } catch (Exception e) {
            log.error("Uncaught error in kafka producer I/O thread: ", e);
        }
    }

    log.debug("Beginning shutdown of Kafka producer I/O thread, sending remaining records.");

    // okay we stopped accepting requests but there may still be
    // requests in the accumulator or waiting for acknowledgment,
    // wait until these are completed.
    while (!forceClose && (this.accumulator.hasUnsent() || this.client.inFlightRequestCount() > 0)) {
        try {
            run(time.milliseconds());
        } catch (Exception e) {
            log.error("Uncaught error in kafka producer I/O thread: ", e);
        }
    }
    if (forceClose) {
        // We need to fail all the incomplete batches and wake up the threads waiting on
        // the futures.
        this.accumulator.abortIncompleteBatches();
    }
    try {
        this.client.close();
    } catch (Exception e) {
        log.error("Failed to close network client", e);
    }

    log.debug("Shutdown of Kafka producer I/O thread has completed.");
}
~~~

我们来看一看真正工作的 run() 方法是什么样的。它有一个参数 `now`，表示现在的时刻。它执行的操作的步骤是这样的：
 - 获取本机缓存的 Cluster 信息，即 metadata 信息。
 - 对 KafkaProducer 的 RecordAccumulator 进行检查，获取 `ReadyCheckResult`，它包含了：
   - 需要发送数据的所有 Node
   - 延时时间（如果有的话）
   - leader 未知的 topic
 - 如果有 leader 未知的 topic，则更新本机的 metadata 即缓存的 Cluster 数据。具体会在后面 NetworkClient.poll() 中进行。
 - 对于所有需要发送数据的 Node，如果与该 Node 没有连接（如果可以连接，会初始化该连接），暂时先移除该 Node。
 - 得到所有可以发送数据的 Node 及其对应的可发送的 RecordBatch 集合（一个 Map<Integer, List<RecordBatch>> 对象，key 是 brokerId，这些 batches 将会在一个 request 中发送）。
 - 保证一个 partition 只有一个 RecordBatch 在发送，保证其有序性。设置 `max.in.flight.requests.per.connection` 为 1 时会生效。
 - 处理 RecordAccumulator 中超时的消息，将由于元数据不可用而导致发送超时的 RecordBatch 移除。
 - 调用 `sendProduceRequests()` 准备发送数据。
 - 调用 `NetworkClient.poll()` 进行真正的网络 I/O 发送之前准备的数据。

其中 `sendProduceRequests()` 和 `NetworkClient.poll()` 是发送数据的关键步骤，我们在后面进行仔细分析。

~~~java
    void run(long now) {
        Cluster cluster = metadata.fetch();
        // get the list of partitions with data ready to send
        RecordAccumulator.ReadyCheckResult result = this.accumulator.ready(cluster, now);

        // if there are any partitions whose leaders are not known yet, force metadata update
        if (!result.unknownLeaderTopics.isEmpty()) {
            // The set of topics with unknown leader contains topics with leader election pending as well as
            // topics which may have expired. Add the topic again to metadata to ensure it is included
            // and request metadata update, since there are messages to send to the topic.
            for (String topic : result.unknownLeaderTopics)
                this.metadata.add(topic);
            this.metadata.requestUpdate();
        }

        // remove any nodes we aren't ready to send to
        Iterator<Node> iter = result.readyNodes.iterator();
        long notReadyTimeout = Long.MAX_VALUE;
        while (iter.hasNext()) {
            Node node = iter.next();
            if (!this.client.ready(node, now)) {
                iter.remove();
                notReadyTimeout = Math.min(notReadyTimeout, this.client.connectionDelay(node, now));
            }
        }

        // create produce requests
        Map<Integer, List<RecordBatch>> batches = this.accumulator.drain(cluster,
                                                                         result.readyNodes,
                                                                         this.maxRequestSize,
                                                                         now);
        if (guaranteeMessageOrder) {
            // Mute all the partitions drained
            for (List<RecordBatch> batchList : batches.values()) {
                for (RecordBatch batch : batchList)
                    this.accumulator.mutePartition(batch.topicPartition);
            }
        }

        List<RecordBatch> expiredBatches = this.accumulator.abortExpiredBatches(this.requestTimeout, now);
        // update sensors
        for (RecordBatch expiredBatch : expiredBatches)
            this.sensors.recordErrors(expiredBatch.topicPartition.topic(), expiredBatch.recordCount);

        sensors.updateProduceRequestMetrics(batches);

        // If we have any nodes that are ready to send + have sendable data, poll with 0 timeout so this can immediately
        // loop and try sending more data. Otherwise, the timeout is determined by nodes that have partitions with data
        // that isn't yet sendable (e.g. lingering, backing off). Note that this specifically does not include nodes
        // with sendable data that aren't ready to send since they would cause busy looping.
        long pollTimeout = Math.min(result.nextReadyCheckDelayMs, notReadyTimeout);
        if (!result.readyNodes.isEmpty()) {
            log.trace("Nodes with data ready to send: {}", result.readyNodes);
            pollTimeout = 0;
        }

        sendProduceRequests(batches, now);

        // if some partitions are already ready to be sent, the select time would be 0;
        // otherwise if some partition already has some data accumulated but not ready yet,
        // the select time will be the time difference between now and its linger expiry time;
        // otherwise the select time will be the time difference between now and the metadata expiry time;
        this.client.poll(pollTimeout, now);
    }
~~~

`RecordAccumulator.ready()` 方法作用是根据当前的 RecordAccumulator 中的 `batches` (一个 Map<TopicPartition, Deque<RecordBatch>> 对象) 找到需要发送数据的所有 Node，不知道其 leader 的所有 topic，以及 `nextReadyCheckDelayMs`（如果需要 delay 的话）。这个方法的返回值类型是 `ReadyCheckResult`。它仅仅是一次检查的结果，将返回如下信息：
 - 需要发送数据的所有 Node。
 - 如果有 delay 的话，距离下次检查的 delayMs。
 - 所有目前不知道 leader 的 topic。

~~~java
public final static class ReadyCheckResult {
    public final Set<Node> readyNodes;
    public final long nextReadyCheckDelayMs;
    public final Set<String> unknownLeaderTopics;
}
~~~

`RecordAccumulator.ready()` 方法用于获取已经可以发送的 RecordBatch 对应的所有 Node。判断一个 RecordBatch 是否可以发送，需要满足下列条件之一：
 - （1）队列中有多个 RecordBatche 或者第一个 RecordBatch 已满。
 - （2）等待是否超时，超时的话则需要发送。
 - （3）是否有其他线程在等待 BufferPool 释放空间, 即 BufferPool 的空间耗尽了。
 - （4）是否有线程在等待 flush 操作完成。`KafkaProducer.flush()` 会将所有的 batch 发送给 broker，不管 RecordBatch 是否已满。
 - （5）Sender 线程准备关闭。

~~~java
    public ReadyCheckResult ready(Cluster cluster, long nowMs) {
        Set<Node> readyNodes = new HashSet<>();
        long nextReadyCheckDelayMs = Long.MAX_VALUE;
        Set<String> unknownLeaderTopics = new HashSet<>();

        boolean exhausted = this.free.queued() > 0;
        for (Map.Entry<TopicPartition, Deque<RecordBatch>> entry : this.batches.entrySet()) {
            TopicPartition part = entry.getKey();
            Deque<RecordBatch> deque = entry.getValue();

            // 找到该 partition 的 leader replica 所在的 Node
            // 因为消息实际上是发送给 leader replica, 在通过同步复制发送到其他 replica 中的
            Node leader = cluster.leaderFor(part);
            synchronized (deque) {
                if (leader == null && !deque.isEmpty()) {
                    // This is a partition for which leader is not known, but messages are available to send.
                    // Note that entries are currently not removed from batches when deque is empty.
                    // 这个 partition 未知, 记录下这个 partition 的信息
                    unknownLeaderTopics.add(part.topic());
                } else if (!readyNodes.contains(leader) && !muted.contains(part)) {
                    // 判断这个节点是否还未加入 readyNodes 集合,
                    // 因为有可能多个 partition 的 leader replica 位于同一个 Node 上.
                    // 取这个 TopicPartition 队列中的第一个 RecordBatch
                    RecordBatch batch = deque.peekFirst();
                    if (batch != null) {
                        boolean backingOff = batch.attempts > 0 && batch.lastAttemptMs + retryBackoffMs > nowMs;
                        long waitedTimeMs = nowMs - batch.lastAttemptMs;
                        long timeToWaitMs = backingOff ? retryBackoffMs : lingerMs;
                        long timeLeftMs = Math.max(timeToWaitMs - waitedTimeMs, 0);
                        // 条件(1)
                        boolean full = deque.size() > 1 || batch.isFull();
                        // 条件(2)
                        boolean expired = waitedTimeMs >= timeToWaitMs;
                        // 条件 (3),(4),(5)
                        boolean sendable = full || expired || exhausted || closed || flushInProgress();
                        if (sendable && !backingOff) {
                            readyNodes.add(leader);
                        } else {
                            // Note that this results in a conservative estimate since an un-sendable partition may have
                            // a leader that will later be found to have sendable data. However, this is good enough
                            // since we'll just wake up and then sleep again for the remaining time.
                            nextReadyCheckDelayMs = Math.min(timeLeftMs, nextReadyCheckDelayMs);
                        }
                    }
                }
            }
        }

        return new ReadyCheckResult(readyNodes, nextReadyCheckDelayMs, unknownLeaderTopics);
    }
~~~

## sendProduceRequests()
sendProduceRequests() 方法用于准备即将发送的数据，第一个参数是一个 Map<Integer, List<RecordBatch>> 对象，key 是 brokerId，value 是发给这个 Node 的 RecordBatch 的集合。它对遍历每一个 Node，执行 sendProduceRequest() 方法。介绍传入的几个参数：
 - entry.getKey() 表示数据的目标 brokerId。
 - entry.getValue() 表示所有发向这个 broker 的 RecordBatch 的集合。
 - acks 由设置中的 `acks` 控制，有 [0, 1, ALL] 三个值。
   - 0 表示发送后不需要服务器端确认
   - 1 表示发送后需要 leader replica 确认
   - ALL 表示发送后需要所有 replica 确认
 - requestTimeout 由设置中的 `timeout.ms` 或 `request.timeout.ms` 控制。

~~~java
private void sendProduceRequests(Map<Integer, List<RecordBatch>> collated, long now) {
    for (Map.Entry<Integer, List<RecordBatch>> entry : collated.entrySet())
        sendProduceRequest(now, entry.getKey(), acks, requestTimeout, entry.getValue());
}
~~~

进一步看这个方法，对于单个 Node 准备发送的数据。它的步骤是：
 - 将单个 broker 对应的 RecordBatch 集合, 重新整理为一个 `Map<TopicPartition, MemoryRecords>` 和一个 `Map<TopicPartition, RecordBatch>`。
 - 根据 produceRecordsByPartition 创建 `ProduceRequest.Builder`。
 - 创建对 ClientResponse 处理的回调方法 callback，它是一个 `RequestCompletionHandler` 对象。
 - 根据 ProduceRequest.Builder 和 callback 创建 `ClientRequest` 对象。
 - 向 NetworkClient 添加 ClientRequest 对象。

~~~java
    private void sendProduceRequest(long now, int destination, short acks, int timeout, List<RecordBatch> batches) {
        Map<TopicPartition, MemoryRecords> produceRecordsByPartition = new HashMap<>(batches.size());
        final Map<TopicPartition, RecordBatch> recordsByPartition = new HashMap<>(batches.size());

        // 将单个 node 对应的 RecordBatch 集合, 重新整理为 produceRecordsByPartition (Map<TopicPartition, MemoryRecords>)
        // 和 recordsByPartition (Map<TopicPartition, RecordBatch>) 两个集合.
        for (RecordBatch batch : batches) {
            TopicPartition tp = batch.topicPartition;
            produceRecordsByPartition.put(tp, batch.records());
            recordsByPartition.put(tp, batch);
        }

        ProduceRequest.Builder requestBuilder =
                new ProduceRequest.Builder(acks, timeout, produceRecordsByPartition);

        RequestCompletionHandler callback = new RequestCompletionHandler() {
            public void onComplete(ClientResponse response) {
                handleProduceResponse(response, recordsByPartition, time.milliseconds());
            }
        };

        String nodeId = Integer.toString(destination);
        ClientRequest clientRequest = client.newClientRequest(nodeId, requestBuilder, now, acks != 0, callback);
        client.send(clientRequest, now);
        log.trace("Sent produce request to {}: {}", nodeId, requestBuilder);
    }
~~~

## NetworkClient.send()

`NetworkClient.send()` 是 NetworkClient 的核心方法之一，它将需要发送的 ClientRequest 对象封装成 KSelector 底层需要的 Send 对象，将 Send 对象放入 brokerId 对应的 `KafkaChannel` 并等待下次 poll() 方法将其发送。它执行的步骤是：
 - 从 ClientRequest 对象获取 nodeId 也就是 brokerId。
 - 验证是否可以向该 node 发送 request。
 - 构造 Request，构造 RequestHeader。
 - 将 Request 对象和 RequestHeader 对象构造成一个 Send 对象。
 - 根据 Send 对象构造 InFlightRequest 对象。
 - 利用 KSelector 发送 Send 对象，也就是 将 Send 对象放入其对应的 KafkaChannel 等待发送。

~~~java
    public void send(ClientRequest request, long now) {
        doSend(request, false, now);
    }

    private void doSend(ClientRequest clientRequest, boolean isInternalRequest, long now) {
        String nodeId = clientRequest.destination();
        if (!isInternalRequest) {
            // If this request came from outside the NetworkClient, validate
            // that we can send data.  If the request is internal, we trust
            // that that internal code has done this validation.  Validation
            // will be slightly different for some internal requests (for
            // example, ApiVersionsRequests can be sent prior to being in
            // READY state.)
            if (!canSendRequest(nodeId))
                throw new IllegalStateException("Attempt to send a request to node " + nodeId + " which is not ready.");
        }
        AbstractRequest request = null;
        AbstractRequest.Builder<?> builder = clientRequest.requestBuilder();
        try {
            NodeApiVersions versionInfo = nodeApiVersions.get(nodeId);
            // Note: if versionInfo is null, we have no server version information. This would be
            // the case when sending the initial ApiVersionRequest which fetches the version
            // information itself.  It is also the case when discoverBrokerVersions is set to false.
            if (versionInfo == null) {
                if (discoverBrokerVersions && log.isTraceEnabled())
                    log.trace("No version information found when sending message of type {} to node {}. " +
                            "Assuming version {}.", clientRequest.apiKey(), nodeId, builder.version());
            } else {
                short version = versionInfo.usableVersion(clientRequest.apiKey());
                builder.setVersion(version);
            }
            // The call to build may also throw UnsupportedVersionException, if there are essential
            // fields that cannot be represented in the chosen version.
            request = builder.build();
        } catch (UnsupportedVersionException e) {
            // If the version is not supported, skip sending the request over the wire.
            // Instead, simply add it to the local queue of aborted requests.
            log.debug("Version mismatch when attempting to send {} to {}",
                    clientRequest.toString(), clientRequest.destination(), e);
            ClientResponse clientResponse = new ClientResponse(clientRequest.makeHeader(),
                    clientRequest.callback(), clientRequest.destination(), now, now,
                    false, e, null);
            abortedSends.add(clientResponse);
            return;
        }
        RequestHeader header = clientRequest.makeHeader();
        if (log.isDebugEnabled()) {
            int latestClientVersion = ProtoUtils.latestVersion(clientRequest.apiKey().id);
            if (header.apiVersion() == latestClientVersion) {
                log.trace("Sending {} to node {}.", request, nodeId);
            } else {
                log.debug("Using older server API v{} to send {} to node {}.",
                    header.apiVersion(), request, nodeId);
            }
        }
        Send send = request.toSend(nodeId, header);
        InFlightRequest inFlightRequest = new InFlightRequest(
                header,
                clientRequest.createdTimeMs(),
                clientRequest.destination(),
                clientRequest.callback(),
                clientRequest.expectResponse(),
                isInternalRequest,
                send,
                now);
        this.inFlightRequests.add(inFlightRequest);
        selector.send(inFlightRequest.send);
    }
~~~

## NetworkClient.poll()
`NetworkClient.poll()` 是真正执行网络 I/O 的地方，它负责发送 request，读取 response，处理新连接，处理断开的连接。
 - 首先更新本地的 metadata。
 - 调用 `KSelector.poll()` 进行网络 I/O 事件的处理。我们之前的文章 [Kafka 网络 I/O 核心 KSelector 分析](https://jintaoguan.github.io/2018/01/20/Kafka-%E7%BB%84%E4%BB%B6-KSelector-%E5%88%86%E6%9E%90/) 已经对这个方法进行过分析，它会对新发生的网络 I/O 事件进行处理和响应。简单地说就是将 KafkaChannel 中待发送的数据发送出去，将未读取的数据读取到 KafkaChannel 中来，并且更新与各个 node 的连接的状态。
 - 对完成的 Send 对象的集合进行处理，对完成的 Receive 对象的集合进行处理
 - 对新断开的连接进行处理，对新断开的连接进行处理。
 - 处理 ApiVersion 请求，处理超时请求。
 - 对所有的 response 调用其 request 的回调方法。

在此不再做详细分析。

~~~java
public List<ClientResponse> poll(long timeout, long now) {
    long metadataTimeout = metadataUpdater.maybeUpdate(now);
    try {
        this.selector.poll(Utils.min(timeout, metadataTimeout, requestTimeoutMs));
    } catch (IOException e) {
        log.error("Unexpected error during I/O", e);
    }

    // process completed actions
    long updatedNow = this.time.milliseconds();
    List<ClientResponse> responses = new ArrayList<>();
    handleAbortedSends(responses);
    handleCompletedSends(responses, updatedNow);
    handleCompletedReceives(responses, updatedNow);
    handleDisconnections(responses, updatedNow);
    handleConnections();
    handleInitiateApiVersionRequests(updatedNow);
    handleTimedOutRequests(responses, updatedNow);

    // invoke callbacks
    for (ClientResponse response : responses) {
        try {
            response.onComplete();
        } catch (Exception e) {
            log.error("Uncaught error in request completion:", e);
        }
    }

    return responses;
}
~~~

## Sender.wakeup()
`Sender.wakeup()` 方法的作用是将 Sender 线程从阻塞中唤醒。这里来看一下它的具体实现：
~~~java
// org.apache.kafka.clients.producer.internals.Sender
public void wakeup() {
    this.client.wakeup();
}

// org.apache.kafka.clients.NetworkClient
@Override
public void wakeup() {
    this.selector.wakeup();
}

// org.apache.kafka.common.network.Selector
@Override
public void wakeup() {
    this.nioSelector.wakeup();
}
~~~

这个方法很简单，其调用过程是下面这个样子：
 - Sender -> NetworkClient -> KSelector -> NSelector

它的作用就是将 Sender 线程从 `select()` 方法的阻塞中唤醒，select() 方法的作用是轮询注册在多路复用器上的 Channel，它会一直阻塞在这个方法上，除非满足下面条件中的一个，否则 select() 将会一直轮询，阻塞在这个地方，直到条件满足。
 - at least one channel is selected;
 - this selector’s `wakeup` method is invoked;
 - the current thread is interrupted;
 - the given timeout period expires.

分析到这里，KafkaProducer 中 dosend() 方法调用 sender.wakeup() 方法作用就很明显的，作用就是：当有新的 RecordBatch 创建后，旧的 RecordBatch 就可以发送了（或者此时有 Metadata 请求需要发送），如果线程阻塞在 select() 方法中，**就将其唤醒，Sender 重新开始运行 run() 方法**，在这个方法中，旧的 RecordBatch （或相应的 Metadata 请求）将会被选中，进而可以及时将这些请求发送出去。



## 总结

至此，Sender 线程发送数据的过程就结束了。其中使用了 Kafka 中的一个重要组件 `NetworkClient` 进行数据的发送，接受以及具体的网络 I/O。NetworkClient 可以看做是客户端的 KSelector 的又一层封装，在 `KafkaController` 中的 `ControllerChannelManager` 中也进行了大量使用，用于 broker 之间的网络通信。我们会在之后再写一篇关于 NetworkClient 的文章对其进行总结。

参考文章
* [Kafka 源码分析之 Producer NIO 网络模型（四）](http://matt33.com/2017/08/22/producer-nio/)
* [Kafka 客户端 Producer 分析](https://jintaoguan.github.io/2017/12/20/Kafka-Producer-%E5%88%86%E6%9E%90/)
* [Kafka 网络 I/O 核心 KSelector 分析](https://jintaoguan.github.io/2018/01/20/Kafka-%E7%BB%84%E4%BB%B6-KSelector-%E5%88%86%E6%9E%90/)
