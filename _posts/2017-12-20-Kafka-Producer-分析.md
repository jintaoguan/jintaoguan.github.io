---
layout:     post
title:      Kafka 客户端 Producer 分析
subtitle:   
date:       2017-12-20
author:     Jintao
header-img: img/post-bg-universe.jpg
catalog: true
tags:
    - kafka
---

## Kafka Producer 使用

我们先来看一看 Kafka Producer 客户端的使用

~~~java
public class Producer extends Thread {
    private final KafkaProducer<Integer, String> producer;
    private final String topic;
    private final Boolean isAsync;

    public Producer(String topic, Boolean isAsync) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("client.id", "DemoProducer");
        props.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producer = new KafkaProducer<Integer, String>(props);
        this.topic = topic;
        this.isAsync = isAsync;
    }

    public void run() {
        int messageNo = 1;
        // 连续发送 100 条消息
        while (messageNo <= 100) {
            String messageStr = "Message_" + messageNo;
            if (isAsync) {  
                // 异步发送消息
                producer.send(new ProducerRecord<Integer, String>(topic, messageNo, messageStr), 
                    new Callback() {
                        public void onCompletion(RecordMetadata metadata, Exception e) {
                            System.out.println("The offset of the record we just sent is: " + metadata.offset());
                        }
                    }
                );
            } else {        
                // 同步发送消息
                producer.send(new ProducerRecord<Integer, String>(topic, messageNo, messageStr)).get();
            }
            ++messageNo;
        }
    }
}
~~~

可以看到，Kafka Producer 的使用非常简单，需要做的只有两点：
 + 配置 Kafka Producer 的参数，比如连接的 Kafka 集群的地址，clientId，key 的序列化器，value 的序列化器。
 + 发送消息，核心函数是 producer.send(new ProducerRecord<Integer, String>(topic, messageNo, messageStr))。
 + 用户可以自定义 Callback 回调函数，可以传入 send() 方法，用户必须为 Callback 回调函数实现 `onCompletion(RecordMetadata metadata, Exception e)` 方法。该回调函数会在消息发送之后自动被调用。
 + 发送消息的返回结果 `RecordMetadata` 记录元数据包括了消息的offset（在哪个partition的哪里offset）

![](/img/post-img/2017-12-20/kafka-client.jpg)

KafkaProducer.send() 方法返回的是一个 `Future<RecordMetadata>`，可以通过这个 Future 实现阻塞和非阻塞的消息发送。
  + 阻塞：在调用 send 返回 Future 时，立即调用 get()，因为 Future.get() 在没有返回结果时会一直阻塞。
  + 非阻塞：提供一个 Callback，调用 send() 后，可以继续发送消息而不用等待。当有结果返回时，Callback 会被自动通知执行。

## Kafka Producer 分析

我们先来看一看 send() 函数内部。很简单，这里 send() 方法会先调用 ProducerInterceptors.onSend() 方法。用户可以自定义 ProducerInterceptor 并配置 `interceptor.classes` 属性使其生效。onSend() 方法返回的是真正需要发送的 record。

~~~java
    public Future<RecordMetadata> send(ProducerRecord<K, V> record, Callback callback) {
        // intercept the record, which can be potentially modified; this method does not throw exceptions
        // 发送 record 之前, 先调用 ProducerInterceptors 的 onSend() 方法，截取发送的 record 做处理。
        ProducerRecord<K, V> interceptedRecord = this.interceptors == null ? record : this.interceptors.onSend(record);
        return doSend(interceptedRecord, callback);
    }
~~~

然后进入 doSend() 方法。onSend() 方法的操作如下
 + 发送数据前，拿到 producer 端的 metadata 数据，可能是直接从服务器端获取，也可以是从客户端的缓存获取。该 metadata 数据在计算 record 归属的 partition 时候用到。
 + 序列化 record 的 key 与 value。
 + 计算出这个 record 被发往的 partition，由三个步骤组成：
   +  如果用户已为 record 显式地设置了 partition, 那么就用用户设置的 partition。
   +  如果 record 没有设置 partition：
      +  如果 record 有 key, 就用 (该 key 的 murmur2 哈希值 % partitions.size()) 来确定 partition。
      +  如果 record 没有 key, 就用 (该 topic 的 record 计数器 % availablePartitions.size()) 来确定 partition。
 + 计算序列化之后的 record (key + value) 所占的字节数，不允许超过阈值
 + 把序列化之后的 record 缓存进 RecordAccumulator 的 buffer 中
 + 如果有 batch 存满了，或者原 batch 剩余空间不足而创建了新的 batch，都要唤醒 sender 线程并发送已满的 batch 中的数据。sender 线程负责客户端与服务器之间的通信，会在以后更详细地进行分析。

~~~java
    /**
     * Implementation of asynchronously send a record to a topic.
     */
    private Future<RecordMetadata> doSend(ProducerRecord<K, V> record, Callback callback) {
        TopicPartition tp = null;
        try {
            // first make sure the metadata for the topic is available
            // 发送数据前，更新 producer 端的 metadata 数据，非常重要
            ClusterAndWaitTime clusterAndWaitTime = waitOnMetadata(record.topic(), record.partition(), maxBlockTimeMs);
            long remainingWaitMs = Math.max(0, maxBlockTimeMs - clusterAndWaitTime.waitedOnMetadataMs);
            Cluster cluster = clusterAndWaitTime.cluster;

            // 序列化 key 和 value
            byte[] serializedKey;
            try {
                serializedKey = keySerializer.serialize(record.topic(), record.key());
            } catch (ClassCastException cce) {
                throw new SerializationException("Can't convert key of class " + record.key().getClass().getName() +
                        " to class " + producerConfig.getClass(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG).getName() +
                        " specified in key.serializer");
            }
            byte[] serializedValue;
            try {
                serializedValue = valueSerializer.serialize(record.topic(), record.value());
            } catch (ClassCastException cce) {
                throw new SerializationException("Can't convert value of class " + record.value().getClass().getName() +
                        " to class " + producerConfig.getClass(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG).getName() +
                        " specified in value.serializer");
            }

            // 计算record的partition的过程, 由3个步骤组成
            // 1. 如果用户已为 record 显式地设置了 partition, 那么就用用户设置的 partition
            // 2. 如果 record 没有设置 partition, 那么由默认的 DefaultPartitioner 来计算 record 的 partition
            //    1). 如果 record 有 key, 就用 (该key的murmur2 hash值 % partitions.size()) 来确定 partition
            //    2). 如果 record 没有 key, 就用 (该topic的record计数器 % availablePartitions.size()) 来确定 partition
            int partition = partition(record, serializedKey, serializedValue, cluster);

            // 计算序列化之后的 record(key/value pair) 所占的字节数
            int serializedSize = Records.LOG_OVERHEAD + Record.recordSize(serializedKey, serializedValue);
            ensureValidRecordSize(serializedSize);

            // 创建 TopicPartition 对象，用于获得 RecordAccumulator 的 buffer 中的 batch 队列
            tp = new TopicPartition(record.topic(), partition);
            long timestamp = record.timestamp() == null ? time.milliseconds() : record.timestamp();
            log.trace("Sending record {} with callback {} to topic {} partition {}", record, callback, record.topic(), partition);

            // producer callback will make sure to call both 'callback' and interceptor callback
            // 这里新建的 InterceptorCallback 对象既包含了 Interceptor 对象(实现了onSend(), onAcknowledgement()和onSendError()),
            // 也包含了 Callback对象 (实现了onCompletion()). 将这个 InterceptorCallback 对象传给 RecordAccumulator, 由它执行
            Callback interceptCallback = this.interceptors == null ? callback : new InterceptorCallback<>(callback, this.interceptors, tp);

            // 把序列化之后的 key 与 value 放入 RecordAccumulator 的 buffer 中
            RecordAccumulator.RecordAppendResult result = accumulator.append(tp, timestamp, serializedKey, serializedValue, interceptCallback, remainingWaitMs);

            // 如果有 batch 存满了，或者原 batch 剩余空间不足而创建了新的 batch,
            // 都要唤醒sender线程并发送已满的batch中的数据
            if (result.batchIsFull || result.newBatchCreated) {
                log.trace("Waking up the sender since topic {} partition {} is either full or getting a new batch", record.topic(), partition);
                this.sender.wakeup();
            }
            return result.future;
            // handling exceptions and record the errors;
            // for API exceptions return them in the future,
            // for other exceptions throw directly
        } catch (ApiException e) {
            // 省略...
        }
    }
~~~

doSend() 方法并不复杂，但是其中包含了四个重要的部分：
 + 从服务器端获取 metadata 信息。
 + 根据 record 计算其对应的 partition。
 + 将 record 缓存进 RecordAccumulator。
 + 唤醒 send() 线程将 record batch 发送给服务器。

我们首先分析一下 Producer 获取 metadata 的过程。在分析之前，我们需要了解 metadata 是什么。每个 Kafka Producer 拥有一个 `private final Metadata metadata;` 成员变量。一个 KafkaProducer 对象只拥有一个 Metadata 对象, 存储了这个 KafkaProducer 所知道的 topic，partition 与 cluster 的信息。Metadata 类被 client 线程（即用户发送消息的线程）和后台 sender 所共享, 所以 Metadata 所有的方法都由 `synchronized` 保证线程安全。Metadata 类只保存了 Kafka 集群部分 topic 的数据，当我们请求一个它上面没有的 topic 的 metadata 时，它会通过发送请求给服务器端来更新 metadata 数据。

Metadata 会在下面两种情况下进行更新：1) KafkaProducer 第一次发送消息时强制更新，其他时间周期性更新，它的周期性更新通过 Metadata 的 `lastRefreshMs`，`lastSuccessfulRefreshMs` 这2个字段来实现。 2) 强制更新：调用 Metadata.requestUpdate() 方法将 Metadata 对象的 `needUpdate` 标志置成 true 来强制更新。

Metadata 类的大多数成员是用于控制 metadata 更新，其中的核心成员 `private Cluster cluster;` 是用来存放具体的 metadata 数据。cluster 记录了 Kafka 集群中的 
 + 所有的 node 的列表
 + topic 与 partition（PartitionInfo) 的关系
 + node 与 partition (PartitionInfo) 的关系
 + brokerId 与 node 的对应关系
 + Kafka 集群的 id

其中最重要的 `PartitionInfo` 类包含了 topic、partition、leader、replicas、ISR 这些信息，是对一个 partition 的描述。Cluster 中所有字段都是 `private final`, 只提供了查询的方法, 不可变对象保证了 `Cluster` 的线程安全。

~~~java
public final class Cluster {

    private final boolean isBootstrapConfigured;
    // Kafka 集群的 node 列表 
    private final List<Node> nodes;
    private final Set<String> unauthorizedTopics;
    private final Set<String> internalTopics;
    // partitionsByTopicPartition 是一个 TopicPartition 到其 PartitionInfo 的查询表
    // 可以根据 TopicPartition 知道这个 partition 的 leader 所在的 Node, 所有 replica 的位置 Node[], 所有 ISR 的位置 Node[]
    private final Map<TopicPartition, PartitionInfo> partitionsByTopicPartition;
    // partitionsByTopic 是一个 Topic 到其所有 PartitionInfo 的查询表
    // 每个 Topic 有多个 Partition, 而每个 Partition 都有其自己的 PartitionInfo
    private final Map<String, List<PartitionInfo>> partitionsByTopic;
    // availablePartitionsByTopic 是一个可用（leader 不为 null）的 Topic 与其所有 Partition 的查询表
    private final Map<String, List<PartitionInfo>> availablePartitionsByTopic;
    // partitionsByNode 是一个 Node 到 PartitionInfo 的查询表
    // 每个 Node 会包含多个 Topic 的多个 Partition
    private final Map<Integer, List<PartitionInfo>> partitionsByNode;
    // nodesById 是一个 Node.id 到 Node 的查询表
    private final Map<Integer, Node> nodesById;
    // Kafka 集群的 id
    private final ClusterResource clusterResource;
    
    // 省略...
}
~~~

## metadata 信息的获取

现在我们再来看看 waitOnMetadata()。 waitOnMetadata() 方法的主要作用是获取这个 topic 的 metadata，返回一个 `ClusterAndWaitTime` 对象，其中包括 `cluster` 和 `waitedOnMetadataMs`，表示 Cluster metadata 和该函数消耗的时间。
 + 首先判断本地有没有这个 topic, 没有则将 metadata 的更新标志设置为 true，表示需要更新 metadata
 + 获取本地缓存的 cluster 信息，如果其中已经有了该 topic 的信息则直接返回缓存的 cluster 信息。
 + 如果本地缓存没有该 topic 的信息，则将 metadata 的更新标志设置为 true，并唤醒 sender 线程从服务器获取 metadata。
 + 根据从服务器得到的最新 cluster 信息验证传入的 partition 是否合法。合法则返回刚刚更新的 cluster 信息。

~~~java
    private ClusterAndWaitTime waitOnMetadata(String topic, Integer partition, long maxWaitMs) throws InterruptedException {
        // add topic to metadata topic list if it is not there already and reset expiry
        // 在 metadata 中添加 topic 后, 如果 metadata 中没有这个 topic 的 meta,
        // 那么 metadata 的更新标志设置为了 true.
        metadata.add(topic);
        // 获取当前的 Cluster 信息, 仅仅是本地缓存的 Cluster 信息
        Cluster cluster = metadata.fetch();
        // 该 Topic 的所有 partition 的数量, 或者 null (即当前没有该 Topic 的 metadata).
        Integer partitionsCount = cluster.partitionCountForTopic(topic);
        // Return cached metadata if we have it, and if the record's partition is either undefined
        // or within the known partition range
        // 如果目前 KafkaProducer 里已经有了该 topic 的 metadata 并且目标 partition 合法,
        // 那么就直接用本地缓存的 cluster metadata 数据, 而不去更新 Metadata.
        if (partitionsCount != null && (partition == null || partition < partitionsCount))
            return new ClusterAndWaitTime(cluster, 0);

        // 这里表示目前 KafkaProducer 没有该 Topic 的 metadata, 或者目标 partition 不合法 (server 端的 partition 增加了).
        long begin = time.milliseconds();
        // 根据最大阻塞时间, 限制 remainingWaitMs, 表示剩余等待时间不可以超过限制, 否则抛出超时异常.
        long remainingWaitMs = maxWaitMs;
        long elapsed;
        // Issue metadata requests until we have metadata for the topic or maxWaitTimeMs is exceeded.
        // In case we already have cached metadata for the topic, but the requested partition is greater
        // than expected, issue an update request only once. This is necessary in case the metadata
        // is stale and the number of partitions for this topic has increased in the meantime.

        // 如果 metadata 中不存在这个 topic 的 metadata, 那么就请求更新 metadata.
        // 如果 metadata 没有更新的话, 方法就一直处在 do ... while 的循环之中, 在循环之中, 主要做以下操作:
        // 1. metadata.requestUpdate() 将 metadata.needUpdate 标志设置为 true(强制更新), 并返回当前的版本号(version),
        //    通过版本号来判断 metadata 是否完成更新.
        // 2. sender.wakeup() 唤醒 sender 线程, sender 线程又会去唤醒 NetworkClient 线程, NetworkClient 线程进行一些实际的操作.
        // 3. metadata.awaitUpdate(version, remainingWaitMs) 等待 metadata 的更新.
        do {
            log.trace("Requesting metadata update for topic {}.", topic);
            // 返回当前版本号, 初始值为0, 每次更新完成时会自增 (Metadata.update()方法中), 并将 needUpdate 设置为 true
            int version = metadata.requestUpdate();
            // 唤起 sender, 发送 metadata 更新请求
            sender.wakeup();
            try {
                // 等待 metadata 完成更新, 这里这是一个阻塞函数
                metadata.awaitUpdate(version, remainingWaitMs);
            } catch (TimeoutException ex) {
                // Rethrow with original maxWaitMs to prevent logging exception with remainingWaitMs
                throw new TimeoutException("Failed to update metadata after " + maxWaitMs + " ms.");
            }
            // 这里获取了更新之后的 cluster 信息
            cluster = metadata.fetch();
            elapsed = time.milliseconds() - begin;
            // 检测超时
            if (elapsed >= maxWaitMs)
                throw new TimeoutException("Failed to update metadata after " + maxWaitMs + " ms.");
            // 认证失败，对当前 topic 没有 Write 权限
            if (cluster.unauthorizedTopics().contains(topic))
                throw new TopicAuthorizationException(topic);
            remainingWaitMs = maxWaitMs - elapsed;
            // 获取这个 topic 的 partition 数量
            partitionsCount = cluster.partitionCountForTopic(topic);
        } while (partitionsCount == null);

        // 判断传入的目标 partition 是否合法
        if (partition != null && partition >= partitionsCount) {
            throw new KafkaException(
                    String.format("Invalid partition given with record: %d is not in the range [0...%d).", partition, partitionsCount));
        }

        return new ClusterAndWaitTime(cluster, elapsed);
    }
~~~

当该方法执行结束，KafkaProducer 即获取了最新的 metadata。当然这里涉及到了 Sender 线程向 Kafka 发送更新请求的操作，在这里暂且略过，以后会详细介绍。

## record 属于哪个 partition

假如用户没有显式地设置 record 的 partition，那么我们就需要根据序列化后的 key 计算出该 record 所归属的 partition。Kafka 中默认的 `DefaultPartitioner` 计算 partition 的基本策略是：
 +  如果用户已为 record 显式地设置了 partition, 那么就用用户设置的 partition。
 +  如果 record 没有设置 partition：
    +  如果 record 有 key, 就用 (该 key 的 murmur2 哈希值 % partitions.size()) 来确定 partition。
    +  如果 record 没有 key, 就用 (该 topic 的 record 计数器 % availablePartitions.size()) 来确定 partition。

有人会问为什么这里没有体现关于 partition leader 的代码？其实这里仅仅是计算 partition。一个 partition 有多个 replica，每个 replica 应该是一样的（假设不考虑 watermark 的延迟），leader 只是其中的一个 replica，只要到最终将数据发送给服务器的时候，发送给 leader 所在的 node 就可以了。我们可以知道 leader 因为 Cluster 中的 PartitionInfo 已经记录了每个 partition 的 leader。实际上这里为 record 选择 partition，只是为了负载均衡，跟 partition 的 leader 没有多大关系。如果一个 topic 只有一个 partition 的话，在集群环境下就不能水平扩展：这个 topic 的消息只能写到一个节点。而为一个 topic 设置多个 partition，可以同时往多个节点的多个 partition 写数据。注意：多个 partition 都是同一个 topic 的，每个 partition 的逻辑意义都是相同的，只是物理位置不同而已。

~~~java
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
        int numPartitions = partitions.size();
        if (keyBytes == null) {
            // record 没有 key 则采用该 topic 的 record 计数器求模
            int nextValue = nextValue(topic);
            // 根据 cluster metadata 获得该 topic 的所有 available partition
            List<PartitionInfo> availablePartitions = cluster.availablePartitionsForTopic(topic);
            if (availablePartitions.size() > 0) {
                int part = Utils.toPositive(nextValue) % availablePartitions.size();
                return availablePartitions.get(part).partition();
            } else {
                // no partitions are available, give a non-available partition
                return Utils.toPositive(nextValue) % numPartitions;
            }
        } else {
            // hash the keyBytes to choose a partition
            // 有 key 的话就用序列化的 key 的 murmur2 哈希值求模
            return Utils.toPositive(Utils.murmur2(keyBytes)) % numPartitions;
        }
    }

    // topic 的 record 计数器，初始值为一个随机数
    private int nextValue(String topic) {
        AtomicInteger counter = topicCounterMap.get(topic);
        if (null == counter) {
            counter = new AtomicInteger(new Random().nextInt());
            // 这里使用的是 ConcurrentMap 的更新方法, 需要判断返回值是否为 null
            AtomicInteger currentCounter = topicCounterMap.putIfAbsent(topic, counter);
            if (currentCounter != null) {
                counter = currentCounter;
            }
        }
        return counter.getAndIncrement();
    }
~~~

## RecordAccumulator 分析
`RecordAccumulator` 是一个客户端缓存区，用于存放序列化的 key 和 value，可以将多条消息缓存起来，等到一个特定时间批量地将消息写到 Kafka 集群中去。RecordAccumulator 维护了一个 `private final ConcurrentMap<TopicPartition, Deque<RecordBatch>> batches;`，这个 Map 的 key 是 TopicPartition，value 是一个双端队列，队列里存放的是消息集合的数据结构 RecordBatch。

![](/img/post-img/2017-12-20/record-accumulator.jpg)

在 doSend() 方法中，很关键的一步是 append() 方法，将序列化之后的 key 和 value 放入 RecordAccumulator 缓存，如果有 batch 已经满了，则唤醒 sender 线程，让其发送给 Kafka 服务器端。我们首先来看一下 append() 方法，append() 方法做了这些事情：
 + 取得该 TopicPartition 对应的 batch 队列，如果该 TopicPartition 还没有 batch 队列，则为其创建一个新的 batch 队列，并返回该队列。
 + 追加 record 到 Deque<RecordBatch> 队列中去，如果返回值 RecordAppendResult 对象不是 null 则说明成功将 record 追加到了队列的最后一个 batch 中。方法结束直接返回。
 + 如果返回值是 null，我们将从 `BufferPool` 中申请一个新的 ByteBuffer 对象，将消息拷贝进该 ByteBuffer，并用这个 ByteBuffer 构建 RecordBatch，并将这个 RecordBatch 加入这个 TopicPartition 所对应的队列。这里有一段代码 `free.deallocate(buffer)`，为什么要回收这个 ByteBuffer 呢？应该多线程导致的问题。如果这里没有回收该 buffer 的代码：假设线程 A 与线程 B 同时为一个 TopicPartition 追加数据，而且该 TopicPartition 是一个新的 TopicPartition，其队列中没有 RecordBatch 对象，那么线程 A 与线程 B 都会创建 ByteBuffer 并且试图去获得锁。假设线程 A 获得锁（线程 B 在这里阻塞），并且成功追加数据，将该 RecordBatch 加入队列，退出函数，释放该锁。之后线程B获得锁，也会将自己的 RecordBatch 加入队列，但是之前线程 A 加入的 RecordBatch 并未写满，造成资源浪费。
 + 将这个新创建的 RecordBatch 加入 `incomplete` 集合，表示这个 RecordBatch 还未写满。
 + 返回结果中包含了 `batchIsFull` 和 `newBatchCreated` 这两个 Boolean 字段，上一层的 doSend() 方法会根据这两个字段决定是否唤醒 sender 线程来将已经写满的 RecordBatch 发送给 Kafka 服务器。

~~~java
    public RecordAppendResult append(TopicPartition tp,
                                     long timestamp,
                                     byte[] key,
                                     byte[] value,
                                     Callback callback,
                                     long maxTimeToBlock) throws InterruptedException {
        // We keep track of the number of appending thread to make sure we do not miss batches in abortIncompleteBatches().
        // 这里是对正在进行追加的 record 进行计数, 当追加完成时(函数最后的finally), 我们看到计数器自减了.
        appendsInProgress.incrementAndGet();
        try {
            // check if we have an in-progress batch
            // 根据 record 的 TopicPartition，确定该 record 应该存入的 batch 队列
            // 1. 如果该 TopicPartition 对应的队列为空，则为这个 TopicPartition 新建一个 batch 队列并返回
            // 2. 如果该 TopicPartition 已存在一个 batch 队列，则返回队列的最后一个 batch 队列
            Deque<RecordBatch> dq = getOrCreateDeque(tp);
            // 这里的 batches 是一个 ConcurrentMap<TopicPartition, Deque<RecordBatch>>，通过 putIfAbsent() 可以保证其线程安全
            // Deque<RecordBatch> 不是线程安全的对象, 所以追加 record 的时候需要使用 synchronize 关键字保证其线程安全
            synchronized (dq) {
                // 这里有可能是因为, 该 KafkaProducer 对象此时已经被其他线程所关闭，所以需要抛出异常.
                if (closed)
                    throw new IllegalStateException("Cannot send after the producer is closed.");
                // 追加 record(这里已经是序列化之后的 key/value pair) 到 Deque<RecordBatch> 中去
                // 如果返回值 RecordAppendResult 对象不是 null 则说明追加操作成功(成功将 record 追加到了队列的最后一个batch中)
                // 如果返回值是 null, 则说明这是一个新创建的 batch 队列, 目前还没有可用的 batch, 我们将为其创建新的 batch 并放入队列.
                RecordAppendResult appendResult = tryAppend(timestamp, key, value, callback, dq);
                if (appendResult != null)
                    return appendResult;
            }

            // we don't have an in-progress record batch try to allocate a new batch
            // 这里我们需要为该 batch 队列创建新的 RecordBatch 并放入队列.
            // 需要初始化相应的 RecordBatch, 要为其分配的大小是: max（batch.size, 包括头文件后的本条消息的大小）
            int size = Math.max(this.batchSize, Records.LOG_OVERHEAD + Record.recordSize(key, value));
            log.trace("Allocating a new {} byte message buffer for topic {} partition {}", size, tp.topic(), tp.partition());
            // 为该 TopicPartition 对应的 RecordBatch 队列 创建一个 ByteBuffer 对象.
            ByteBuffer buffer = free.allocate(size, maxTimeToBlock);
            // 同样地, 为了防止其他线程同样为其创建 ByteBuffer, 在这里对这个队列加锁.
            synchronized (dq) {
                // Need to check if producer is closed again after grabbing the dequeue lock.
                // 再次查看是否 KafkaProducer 已经被关闭.
                if (closed)
                    throw new IllegalStateException("Cannot send after the producer is closed.");
                // 追加成功表示这个 queue 已经存在, 那么就释放这个已经分配的空间
                // 这里应该同样是多线程导致的问题, 假设线程A与线程B同时为一个 TopicPartition 追加数据,
                // 而且该 TopicPartition 是一个新的 TopicPartition, 其队列中没有 RecordBatch 对象,
                // 那么线程 A 与线程 B 都会创建 ByteBuffer 并且试图去获得锁,
                // 假设线程 A 获得锁(线程 B 在这里阻塞), 并且成功追加数据, 将该 RecordBatch 加入队列, 退出函数, 释放该锁.
                // 之后线程 B 获得锁, 也会将自己的 RecordBatch 加入队列, 但是之前线程 A 加入的 RecordBatch 并未写满, 造成资源浪费
                RecordAppendResult appendResult = tryAppend(timestamp, key, value, callback, dq);
                if (appendResult != null) {
                    // Somebody else found us a batch, return the one we waited for! Hopefully this doesn't happen often...
                    free.deallocate(buffer);
                    return appendResult;
                }

                // 将新创建的 ByteBuffer 对象包装成 MemoryRecords 对象,
                // 然后再包装成 RecordBatch 对象, 并加入这个 TopicPartition 所对应的队列.
                MemoryRecordsBuilder recordsBuilder = MemoryRecords.builder(buffer, compression, TimestampType.CREATE_TIME, this.batchSize);
                RecordBatch batch = new RecordBatch(tp, recordsBuilder, time.milliseconds());
                // 在新创建的 RecordBatch 中追加 record, 并将这个 RecordBatch 对象添加到 batches 集合中.
                // 创建一个 RecordMetadata 的 Future, 用于非阻塞的函数调用.
                // 这里的 FutureRecordMetadata 就是 Future<RecordMetadata> 的一个增强的实现.
                FutureRecordMetadata future = Utils.notNull(batch.tryAppend(timestamp, key, value, callback, time.milliseconds()));

                // 加入这个 TopicPartition 所对应的队列.
                dq.addLast(batch);
                // 该 RecordBatch 还未写满, 放入 IncompleteRecordBatches 集合.
                incomplete.add(batch);
                // 如果（dp.size() > 1), 就证明这个 queue 有一个 batch 是可以发送了
                return new RecordAppendResult(future, dq.size() > 1 || batch.isFull(), true);
            }
        } finally {
            appendsInProgress.decrementAndGet();
        }
    }
~~~

这里使用的 RecordAccumulator 的 tryAppend() 方法就是将 record 放入 RecordBatch 队列的最后一个 RecordBatch 中。如果该队列没有 RecordBatch 对象，则直接返回 null 表示追加失败。所以在上面的 append() 方法中多次使用了 tryAppend() 方法来判断 RecordBatch 队列是否已经存在一个可用的 RecordBatch 来追加数据。

~~~java
    // 追加数据, 将 record 放入其 TopicPartition 的 RecordBatch 队列的最后一个 RecordBatch 中.
    private RecordAppendResult tryAppend(long timestamp, byte[] key, byte[] value, Callback callback, Deque<RecordBatch> deque) {
        RecordBatch last = deque.peekLast();
        if (last != null) {
            FutureRecordMetadata future = last.tryAppend(timestamp, key, value, callback, time.milliseconds());
            if (future == null)
                last.close();
            else
                return new RecordAppendResult(future, deque.size() > 1 || last.isFull(), false);
        }
        return null;
    }
~~~

RecordAccumulator 的粗略结构图：

![](/img/post-img/2017-12-20/record-accumulator-2.png)

## BufferPool 分析
可能有人注意到了在 RecordAccumulator 的 append() 方法中使用了 `free.allocate()` 和 `free.deallocate()` 来进行 ByteBuffer 的申请与回收。在 RecordAccumulator 的定义当中我们可以看到 `private final BufferPool free;` ，free 实际是一个 `BufferPool` 对象。那么 BufferPool 是什么呢？

ByteBuffer 的创建与释放非常消耗资源，为了实现内存的高效利用，Kafka 客户端使用 BufferPool 来实现 ByteBuffer 的复用。每个 BufferPool 对象只针对特定大小的（poolableSize 变量指定）ByteBuffer 进行管理，其他大小的 ByteBuffer 并不会缓存进 BufferPool。一般情况下，我们通过调节 RecordAccumulator.batchSize 变量，使一个 MemoryRecords 对象可以存储多个 record。但是如果一条 record 过大，超过 MemoryRecords，那么就不会复用 BufferPool 中缓存的 ByteBuffer，而是额外单独分配 ByteBuffer, 使用完之后也不会放入 BufferPool 进行管理，而是直接丢弃给 GC。

我们先来看一下 BufferPool 的数据结构：

~~~java
public final class BufferPool {
    // 整个 BufferPool 的大小
    private final long totalMemory;
    // ByteBuffer 的大小, BufferPool 管理的所有的 ByteBuffer 的大小都是 poolableSize
    private final int poolableSize;
    private final ReentrantLock lock;
    // 一个 ArrayDeque<ByteBuffer> 队列, 缓存的空闲的相同大小的 ByteBuffer
    private final Deque<ByteBuffer> free;
    // 所有阻塞线程的 Condition 对象
    private final Deque<Condition> waiters;
    // 剩余的可用空间大小
    private long availableMemory;
    private final Metrics metrics;
    private final Time time;
    private final Sensor waitTime;
}
~~~

## Sender 线程分析
`Sender` 线程牵涉到了许多 Kafka 网络通信的内容。由于内容较多，我们会在以后进行更加详细的分析。在这里先留一个坑。


至此，Kafka Producer 的工作原理分析结束。
