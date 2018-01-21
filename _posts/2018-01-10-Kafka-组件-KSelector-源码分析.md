---
layout:     post
title:      Kafka 网络 I/O 核心 KSelector 分析
subtitle:   Kafka 网络通信基础 (2)
date:       2018-01-10
author:     Jintao
header-img: img/post-bg-universe.jpg
catalog: true
tags:
    - kafka
---

## Java NIO 操作类型
Selector 是 Java NIO 中的一个重要组件，用于检查一个或多个 NIO Channel 的状态是否处于可读、可写等。如此可以实现单线程管理多个 Channel，也就是可以管理多个网络链接。Kafka 基于 Java 原生的 NIO Selector，封装了其底层的组件，即 `org.apache.kafka.common.network.Selector` 类。为了区别于 Java 原生的 Selector 与 Kafka 重新封装的 Selector，我们将按照《Apache Kafka源码剖析》一书的方法，将前者称为 `NSelector`，将后者称为 `KSelector`。阅读这一篇文章之前需要掌握 Java NIO 的相关基础知识，推荐阅读 O'Reilly 出版的 《Java NIO》一书的相关章节。

首先我们了解一下 NSelector 相关的事件：

Channel 类型 | OP_CONNECT | OP_ACCEPT | OP_READ | OP_WRITE  
:---: | :---: | :---: | :---: | :---: 
服务器端 ServerSocketChannel |   | x |   |   | 
服务器端 SocketChannel |   |   | x | x
客户端 SocketChannel | x |   | x | x

客户端请求连接，服务器端接受连接，客户端与服务器端开始相互发送消息（读写），按这个逻辑上表就容易理解。为了更深入理解，我们可以看看每个操作类型的就绪条件。

操作类型 | 就绪条件及说明 | 处理方法
:---: | :---: | :---:
OP_CONNECT | 当 SocketChannel.connect() 请求连接**成功**后就绪。该操作只给客户端使用。 | 调用 SocketChannel.finishConnect() 建立连接。
OP_ACCEPT | 当接收到一个客户端连接请求时就绪。该操作只给服务器端使用。 | 调用 ServerSocketChannel.accept() 方法接受客户端连接。
OP_READ | 当操作系统读缓冲区有数据可读时就绪。并非时刻都有数据可读，所以一般需要注册该操作，仅当有就绪时才发起读操作，有的放矢，避免浪费CPU。| 调用 SocketChannel.read() 方法从 SocketChannel 读取数据到 ByteBuffer。
OP_WRITE | 当操作系统写缓冲区有空闲空间时就绪。一般情况下写缓冲区都有空闲空间，小块数据直接写入即可，没必要注册该操作类型，否则该条件不断就绪浪费CPU；但如果是写密集型的任务，比如文件下载等，缓冲区很可能满，注册该操作类型就很有必要，同时注意写完后取消注册。| 调用 SocketChannel.write() 方法将 ByteBuffer 数据写入 SocketChannel。

我们可以通过 NSelector 对这些事件监听，获得触发的 I/O 事件并处理这些事件，从而实现非阻塞的网络 I/O。这个过程中有重要的五类处理 I/O 的方法：
 + 调度：`NSelector.select()` 和 `NSelector.selectKeys()` 方法获取触发的 I/O 事件
 + 发起连接：`SocketChannel.connect()` 方法
 + 接受连接：`ServerSocketChannel.accept()` 方法
 + 写入内容：`SocketChannel.write()` 方法
 + 读出内容：`SocketChannel.read()` 方法

类比地，在 Kafka 中，KSelector 通过对 NSelector 的封装也使用类似的方法进行非阻塞的网络 I/O。
 + 调度：`KSelector.poll()` 方法中封装了 `NSelector.select()` 和 `NSelector.selectKeys()` 方法
 + 发起连接：`KSelector.connect()` 方法中封装了 `SocketChannel.connect()` 方法
 + 接受连接：`Acceptor.accept()` 方法中封装了 `ServerSocketChannel.accept()` 方法 
 + 写入内容：`KafkaChannel.write()` 方法中封装了 `SocketChannel.write()` 方法
 + 读出内容：`KafkaChannel.read()` 方法中封装了 `SocketChannel.read()` 方法


## poll() 方法分析
我们可以先来看一下 KSelector 的一些重要的内部成员：
~~~java
public class Selector implements Selectable {

    // Kselector 内部维护了一个 NSelector，nioSelector 是 Java 原生的 NIO Selector，用于监听网络 I/O 的事件。
    private final java.nio.channels.Selector nioSelector;
    // 维护了 NodeId 与 KafkaChannel 之间的映射关系, 表示生产者客户端与 Node 之间的网络连接
    // KafkaChannel 是在 SocketChannel 上层的封装, 描述对客户端连接的 SocketChannel
    private final Map<String, KafkaChannel> channels;
    // 记录已经完全发送出去的请求
    private final List<Send> completedSends;
    // 记录已经完全接收到的请求
    private final List<NetworkReceive> completedReceives;
    private final Map<KafkaChannel, Deque<NetworkReceive>> stagedReceives;
    // 用来记录调用 SocketChannel.connect() 后立刻建立连接的 SelectionKey, 因为它们不会触发 nioSelector 的 OP_CONNECT 事件
    private final Set<SelectionKey> immediatelyConnectedKeys;
    // 正在关闭的 socketChannel 的集合
    private final Map<String, KafkaChannel> closingChannels;
    // 记录一次 poll() 过程中发现的断开连接的 nodeId
    private final List<String> disconnected;
    // 记录一次 poll() 过程中发现的新建立连接的 nodeId
    private final List<String> connected;
    // 记录发送的请求失败了的 nodeId
    private final List<String> failedSends;
    
    // 略过 ...
    private final Time time;
    private final SelectorMetrics sensors;
    private final String metricGrpPrefix;
    private final Map<String, String> metricTags;
    private final ChannelBuilder channelBuilder;
    private final int maxReceiveSize;
    private final boolean metricsPerConnection;
    private final IdleExpiryManager idleExpiryManager;
    // 省略 ...
}
~~~

KSelector 的工作方式与 NSelector 的工作方式一样，先将 SocketChannel 注册到 selector 上并关注 I/O 事件，然后通过对 selector 轮询得到新发生的 I/O 事件并对其进行处理。KSelector 就是在 `poll()` 方法中完成一次工作周期。poll() 方法是真正执行网络 I/O 的地方, 它会调用 `select()` 方法取得发生的 I/O 事件并处理这个 I/O 事件。
 + 当 Channel 可写的时候, 发送 KafkaChannel.send 字段, 一次只能发送一个 Send 数据, 甚至一个也发送不完, 需要多次 poll 完成。
 + 当 Channel 可读的时候, 读取数据到 KafkaChannel.receive 字段, 读取到一个完整的 NetworkReceive 后, 将其缓存到 stagedReceives 中。
 + 当一次 pollSelectionKeys 完成后, 会将 `stagedReceives` 中的数据转移到 `completedReceives` 中去。
 + 最后调用 `maybeCloseOldestConnection()` 方法关闭长期空闲的连接。


~~~java
    public void poll(long timeout) throws IOException {
        if (timeout < 0)
            throw new IllegalArgumentException("timeout should be >= 0");

        // 清空上一次 poll() 的结果
        clear();

        if (hasStagedReceives() || !immediatelyConnectedKeys.isEmpty())
            timeout = 0;

        /* check ready keys */
        long startSelect = time.nanoseconds();
        // 实际上调用 nioSelector 的 select() 方法
        int readyKeys = select(timeout);
        long endSelect = time.nanoseconds();
        this.sensors.selectTime.record(endSelect - startSelect, time.milliseconds());

        // 处理 I/O 事件的核心方法
        if (readyKeys > 0 || !immediatelyConnectedKeys.isEmpty()) {
            // 先处理 select() 方法产生的 SelectedKeys
            pollSelectionKeys(this.nioSelector.selectedKeys(), false, endSelect);
            // 再处理 connect() 方法中立刻连接上的 socketChannel (因为立刻连接成功, selector 不会收到 OP_CONNECT 事件)
            pollSelectionKeys(immediatelyConnectedKeys, true, endSelect);
        }

        // 将 stagedReceives 中的数据转移到 completedReceives 中去
        addToCompletedReceives();

        long endIo = time.nanoseconds();
        this.sensors.ioTime.record(endIo - endSelect, time.milliseconds());

        // we use the time at the end of select to ensure that we don't close any connections that
        // have just been processed in pollSelectionKeys
        // 关闭长期空闲的连接
        maybeCloseOldestConnection(endSelect);
    }
~~~

poll() 方法中有一个 select() 的调用，其实就是很简单地调用其封装的 NSelector 对象（`nioSelector`）的 select() 方法，如下：

~~~java
    private int select(long ms) throws IOException {
        if (ms < 0L)
            throw new IllegalArgumentException("timeout should be >= 0");
        if (ms == 0L)
            return this.nioSelector.selectNow();
        else
            return this.nioSelector.select(ms);
    }
~~~

所以，我们可以看到 poll() 方法的执行步骤大概是这样的。

 + 首先清空上一次 poll() 调用时候产生的一些内容，我们不再关心，只需要清除它们，在此不做详细分析。简单地说，`clear()` 方法会清除已完成发送的请求集合（`completedSends`），清除已完成接收的请求集合（`completedReceives`），上一次 poll() 发现的新连接上的 nodeId 集合，上一次 poll() 发现的新断开连接的 nodeId 集合，并关闭相关的 SocketChannel。
 + 查看是否有当前可以处理的连接或者 receive 请求。如果有的话那么让 timeout = 0，使后面 select() 无阻塞立刻返回。
 + 首先调用 select() 方法得到新产生的 I/O 事件的个数。
 + 如果有新发生的 I/O 事件或者有新建立的连接就对这些 I/O 事件进行处理。这里需要注意的是 `immediatelyConnectedKeys` 集合中保存了新建立的连接（客户端到服务器端），为什么这些连接不会被 select() 算在新发生的 I/O 事件中呢？**因为客户端向服务器端发起连接的时候会调用 `connected = socketChannel.connect(address)`，connected 如果是 true 表示立刻就成功建立了连接，之后将不会触发 OP_CONNECT 事件。如果 connected 是 false，就表示正在发起连接，目前不知道是否连接成功，需要以后在 OP_CONNECT 事件被触发的时候调用 SocketChannel 的 finishConnect() 判断连接是否成功建立。**
 + 先调用 `pollSelectionKeys()` 方法处理新发生 I/O 事件。
 + 再调用 `pollSelectionKeys()` 方法处理 `immediatelyConnectedKeys`（即立刻成功建立的连接）
 + 将 `stagedReceives` 中的数据转移到 `completedReceives` 中去。
 + 关闭长期空闲的连接。

具体处理 I/O 事件的是 pollSelectionKeys() 方法，也是整个 KSelector 的核心调度的区域。我们来分析一下 pollSelectionKeys() 所做的事情。pollSelectionKeys() 做了下面这些事情：
 
 + 首先取得 SelectionKey 的 Iterator，然后对需要处理的 I/O 所有事件 SelectionKey 进行遍历。在这一步需要注意的是，当我们处理 SelectionKey 集合的时候，SelectionKey 是和 selector 绑定的，我们需要手动调用 `iterator.remove()` 去移除处理过的 SelectionKey。selector 不会自己从已选择键集中移除 SelectionKey 实例，必须在处理完时自己手动移除。下次该 Channel 变成就绪时，Selector 会再次将其放入 SelectedKey 集合中。
 + 对于每个 SelectionKey，首先获取它的附件（一个 KafkaChannel 对象）。需要说明的是，KafkaChannel 封装了 SocketChannel 以及它的 SelectionKey，我们就可以从 SelectionKey 得到 KafkaChannel，也可以反方向地从 KafkaChannel 找到 SelectionKey。更重要的是 KafkaChannel 封装了 read() 和 write() 方法。对每个 SelectionKey，我们根据其类型分别进行处理：
 
   + 如果 `isImmediatelyConnected || key.isConnectable()`，表示该 SocketChannel 请求连接成功后就绪。我们调用 `finishConnect()` 完成剩下连接过程。如果阅读 KafkaChannel 的 finishConnect() 方法，我们可以看到其不仅仅调用了原生 SocketChannel 的 finishConnect() 方法，也会通过调用取消关注该 SocketChannel 的 OP_CONNECT 事件, 转而开始关注 OP_READ 事件。作用就是连接已经建立成功了，以后我们只要监听网络数据的读写就可以了。之后将这个 nodeId 加入 `connected` 集合。如果finishConnect() 方法返回 false 表示连接并没有建立成功，则直接跳过，继续处理余下的 SelectionKey。（问题：为什么直接跳过？等待下次触发 OP_CONNECT 时会继续尝试连接？）
   + 调用 KafkaChannel 的 `prepare()` 方法进行身份验证
   + 如果 `channel.ready() && key.isReadable() && !hasStagedReceive(channel)`，表示 Channel 可读数据就绪。则反复调用 `channel.read()` 方法从 SocketChannel 中读取数据到 KafkaChannel.receive 字段，直到没有剩余数据或者没有完整地读取一个 NetworkReceive 为止。每次完整读取 NetworkReceive 对象后则将其加入 stagedReceives 集合。
   + 如果 `channel.ready() && key.isWritable()`，表示 Channel 可写就绪。调用 `channel.write()` 将数据写入 SocketChannel。如果返回的是 null，表示没有完整写入。如果返回的是一个 Send（通常是一个 NetworkSend 对象），表示写入 Send 成功，则加入 `completedSends` 集合。
   + 如果 `!key.isValid()` 表示这个 SelectionKey 不再有效，我们关闭这个 SelectionKey 所对应的 Channel 并清除其相关数据。
   + 如果工作周期中发生异常，则关闭这个 SelectionKey 所对应的 Channel 并清除其相关数据。

~~~java
    // 处理 I/O 事件的核心方法
    private void pollSelectionKeys(Iterable<SelectionKey> selectionKeys,
                                   boolean isImmediatelyConnected,
                                   long currentTimeNanos) {
        Iterator<SelectionKey> iterator = selectionKeys.iterator();
        while (iterator.hasNext()) {
            SelectionKey key = iterator.next();
            iterator.remove();
            // 获取 SelectionKey 的 attachment, 就是这个 SelectionKey 对应的 KafkaChannel 对象
            // 之前发起连接时, 将 KafkaChannel 放入 SelectionKey 的附件中, 就是为了在这里获取
            KafkaChannel channel = channel(key);

            // register all per-connection metrics at once
            sensors.maybeRegisterConnectionMetrics(channel.id());
            if (idleExpiryManager != null)
                idleExpiryManager.update(channel.id(), currentTimeNanos);

            try {

                /* complete any connections that have finished their handshake (either normally or immediately) */
                // 这部分是对连接成功事件进行处理, 两种情况, 1) 有 OP_CONNECT 事件 2) 发起连接时, 立刻连接成功
                if (isImmediatelyConnected || key.isConnectable()) {
                    // finishConnect() 方法先检测 socketChannel 的连接是否建立完成
                    // 如果连接建立成功则取消关注 OP_CONNECT 事件, 开始关注 OP_READ 事件
                    if (channel.finishConnect()) {
                        // 将该连接的 nodeId 加入 connected 集合
                        this.connected.add(channel.id());
                        this.sensors.connectionCreated.record();
                        SocketChannel socketChannel = (SocketChannel) key.channel();
                        log.debug("Created socket with SO_RCVBUF = {}, SO_SNDBUF = {}, SO_TIMEOUT = {} to node {}",
                                socketChannel.socket().getReceiveBufferSize(),
                                socketChannel.socket().getSendBufferSize(),
                                socketChannel.socket().getSoTimeout(),
                                channel.id());
                    } else
                        // 连接未完成, 则跳过对这个 SelectionKey 对象的处理
                        continue;
                }

                /* if channel is not ready finish prepare */
                // 调用 KafkaChannel.prepare() 方法进行身份验证
                if (channel.isConnected() && !channel.ready())
                    channel.prepare();

                /* if channel is ready read from any connections that have readable data */
                // 处理 OP_READ 事件的核心逻辑
                if (channel.ready() && key.isReadable() && !hasStagedReceive(channel)) {
                    // 这里做的事情是读取一个或多个完整的 NetworkReceive 对象并将其添加到 stagedReceives 中保存
                    // 如果读取不到一个完整的 NetworkReceive 则返回 null, 下次处理 OP_READ 时, 继续读取
                    NetworkReceive networkReceive;
                    while ((networkReceive = channel.read()) != null)
                        addToStagedReceives(channel, networkReceive);
                }

                /* if channel is ready write to any sockets that have space in their buffer and for which we have data */
                // 处理 OP_WRITE 事件的核心逻辑
                if (channel.ready() && key.isWritable()) {
                    // 这里做的事情调用 write() 方法将 KafkaChannel 的 send 字段发送出去, 如果没有发送完成, 则返回 null
                    // 如果发送完成则返回 Send 对象, 加入 completedSends 集合, 等待后续处理
                    Send send = channel.write();
                    if (send != null) {
                        this.completedSends.add(send);
                        this.sensors.recordBytesSent(channel.id(), send.size());
                    }
                }

                /* cancel any defunct sockets */
                // 如果这个 key 不再有效则关闭该 socketChannel
                if (!key.isValid())
                    close(channel, true);

            } catch (Exception e) {
                String desc = channel.socketDescription();
                if (e instanceof IOException)
                    log.debug("Connection with {} disconnected", desc, e);
                else
                    log.warn("Unexpected error from {}; closing connection", desc, e);
                // 如果抛出异常则关闭该 socketChannel
                close(channel, true);
            }
        }
    }
~~~ 

## connect() 方法分析
`connect()` 方法用于客户端向服务器端发起网络连接（只限于客户端端使用）。需要注意的是，这里仅仅是作为客户端发起连接并创建相关的数据结构，具体的建立网络连接的工作是在 poll() 方法中 nioSelector 检测到 OP_CONNECT 事件后，调用 finishConnect() 来完成的，这就是 Java NIO 的一贯做法。connect() 方法是作为客户端时才会调用的方法，仅仅在 `NetworkClient` 类中被使用到，我们以后会进一步分析它的使用。这里 connect() 方法比较简单，有以下几个主要步骤：

 + 调用 `SocketChannel.open()` 创建一个客户端 SocketChannel 对象。
 + 将创建的 SocketChannel 对象设置成非阻塞模式，并根据参数设置其对应 Socket 的发送缓冲区和接收缓冲区的大小。
 + 调用 `SocketChannel.connect()` 发起一个客户端的网络连接。
 + 将这个 SocketChannel 对象注册到 nioSelector 上，并监听其 OP_CONNECT 事件。
 + 创建 KafkaChannel 对象，并将这个 KafkaChannel 对象放入 SelectionKey 的附件，这样以后可以通过 SelectionKey 拿到 KafkaChannel。
 + 对于立刻返回的连接，即 SocketChannel.connect() 方法返回 true，nioSelector 以后将不会受到其 OP_CONNECT 方法，所以我们将这种 SelectionKey 放入 `immediatelyConnectedKeys` 集合方便以后进行处理，并调用 `key.interestOps(0)` 方法，不再监听这个 SocketChannel 的任何 I/O 事件。 
 + **疑问**：Java NIO 通常的做法是先将 SocketChannel 注册到 selector 上并监听 OP_CONNECT，再调用 connect() 发起连接。而这里是先调用 connect() 发起连接，之后再注册到 selector 上监听 OP_CONNECT，并且专门设置了 `immediatelyConnectedKeys` 集合用于处理 connect() 返回 true 的情况。为什么这样做？如果先注册再发起连接，是否就不要 immediatelyConnectedKeys 集合了？
 
~~~java
    // 客户端用法, 使用 KSelector 管理到多个 node 的连接并监听相应的 OP_CONNECT 方法
    // 创建 KafkaChannel, 并添加到 KSelector 的 channels 集合中保存
    @Override
    public void connect(String id, InetSocketAddress address, int sendBufferSize, int receiveBufferSize) throws IOException {
        if (this.channels.containsKey(id))
            throw new IllegalStateException("There is already a connection for id " + id);

        // 创建客户端的 SocketChannel
        SocketChannel socketChannel = SocketChannel.open();
        // 配置成非阻塞模式, 只有非阻塞模式才能注册到 selector 上
        socketChannel.configureBlocking(false);
        Socket socket = socketChannel.socket();
        // 设置为长连接
        socket.setKeepAlive(true);
        if (sendBufferSize != Selectable.USE_DEFAULT_BUFFER_SIZE)
            socket.setSendBufferSize(sendBufferSize);
        if (receiveBufferSize != Selectable.USE_DEFAULT_BUFFER_SIZE)
            socket.setReceiveBufferSize(receiveBufferSize);
        socket.setTcpNoDelay(true);
        boolean connected;
        try {
            // 连接服务端, 注意这里并没有开始真正连接, 或者说因为是非阻塞方式, 是发起一个连接
            // 因为 socketChannel 设置成了非阻塞模式, 这里是一个非阻塞方法调用
            // 如果 connected 为 true, 表示这次连接刚刚发起就连接成功了
            // connect() 方法返回 false 表示不知道连接是否成功, 因为有可能连接正在进行
            // 在后面会通过调用 finishConnect() 方法确认连接是否建立了
            connected = socketChannel.connect(address);
        } catch (UnresolvedAddressException e) {
            socketChannel.close();
            throw new IOException("Can't resolve address: " + address, e);
        } catch (IOException e) {
            socketChannel.close();
            throw e;
        }
        // 将该 socketChannel 注册到 nioSelector 上, 并监听其 OP_CONNECT 事件
        SelectionKey key = socketChannel.register(nioSelector, SelectionKey.OP_CONNECT);
        // 会创建 KafkaChannel 对象, KafkaChannel 是对一个 node 连接的封装
        KafkaChannel channel = channelBuilder.buildChannel(id, key, maxReceiveSize);
        // 将 KafkaChannel 放入 SelectionKey 的附件
        key.attach(channel);
        // KSelector 维护了每个 nodeConnectionId 以及其对应的 KafkaChannel
        this.channels.put(id, channel);

        // 如果连接立刻成功建立了, 那么就不会触发 nioSelector 的 OP_CONNECT 事件, 需要单独处理这次连接
        if (connected) {
            // OP_CONNECT won't trigger for immediately connected channels
            log.debug("Immediately connected to node {}", channel.id());
            immediatelyConnectedKeys.add(key);
            // 这里(暂时)清空 interestOps, 表示 selector.select() 会忽略这个 key
            // 因为已经立刻连接成功了, 所以不会发生 OP_CONNECT 事件
            key.interestOps(0);
        }
    }
~~~

## accept() 方法分析
`accept()` 方法用于 OP_ACCEPT 事件触发后，服务器端接受客户端发起的连接（只限于服务器端使用）。`ServerSocket` 类封装了 `ServerSocketChannel.accept()` 方法。我们在之前的文章 [《Kafka 组件 SocketServer 分析》](https://jintaoguan.github.io/2018/01/02/Kafka-%E7%BB%84%E4%BB%B6-SocketServer-%E6%BA%90%E7%A0%81%E5%88%86%E6%9E%90/) 中已经对其做过分析。大概地说，就是 `SocketServer.run()` 启动 Accepter 对象绑定服务器端的地址和端口，`Accepter.run()` 中使用 NSelector 监听 I/O 事件， `Accepter.accept()` 接受客户端的连接，并将 accept() 方法返回的服务器端 SocketChannel 对象交给 Processor 类处理。具体内容在此略过。

## write() 方法分析
`write()` 方法用于 OP_WRITE 事件触发后，KafkaChannel 对象向 SocketChannel 写入数据（服务器端，客户端都可以使用）。不同于传统的 socket 编程，在 NIO 的工作方式中，我们不是直接向 socket 写入数据，而是先将数据放入一个 ByteBuffer，然后在 OP_WRITE 事件触发后（Socket 的写缓冲区有空闲空间时就绪），调用 SocketChannel.write(ByteBuffer) 将 ByteBuffer 中的数据写入 SocketChannel，从而实现数据的发送。下面的代码就是用户发送数据时调用的 send() 方法，这里并不是真的立刻发送 Send 对象，而是将这个 Send 复制给对应的 KafkaChannel 对象中的 send 字段，相当于暂时的缓存。

~~~java
    // 将 send 放入这个 nodeId 对应的 KafkaChannel 并等待下次 poll() 方法将其发送
    public void send(Send send) {
        String connectionId = send.destination();
        if (closingChannels.containsKey(connectionId))
            this.failedSends.add(connectionId);
        else {
            KafkaChannel channel = channelOrFail(connectionId, false);
            try {
                channel.setSend(send);
            } catch (CancelledKeyException e) {
                this.failedSends.add(connectionId);
                close(channel, false);
            }
        }
    }
~~~

KafkaChannel 的 `setSend()` 方法功能比较简单：
 + 将需要发送的 Send 对象赋值给 KafkaChannel 的 send 字段。每个 KafkaChannel 对象只能缓存 1 个 Send 对象，如果 `KafkaChannel.send != null`，则表示上次缓存的 Send 对象还没有发送出去，那么就会抛出异常。
 + 再将这个 KafkaChannel 的 SelectionKey 的 InterestOps 设置成 OP_WRITE，因为 KafkaChannel 中的 SelectionKey 是和 nioSelector 绑定的，那么这样做会使得该 nioSelector 开始监听 OP_WRITE 事件。在下一次调用 KSelector.poll() 方法时，nioSelector 会接收到触发的 OP_WRITE 事件。回顾之前的 `pollSelectionKeys()` 方法，我们可以看到对 OP_WRITE 事件的处理就是调用 KafkaChannel.write() 方法。

~~~java
    // 设置这个 KafkaChannel 的 send 字段并关注其对应 SocketChannel 的 OP_WRITE 事件
    // 这样当 SocketChannel 的 OP_WRITE 事件发生时，KSelector 就发送 Send
    public void setSend(Send send) {
        // 如果之前还有 send 没有发送完则抛出异常
        if (this.send != null)
            throw new IllegalStateException("Attempt to begin a send operation with prior send operation still in progress.");
        this.send = send;
        this.transportLayer.addInterestOps(SelectionKey.OP_WRITE);
    }
~~~

在前一节的 connect() 方法中，当 SocketChannel.connect() 立刻建立连接之后，我们调用了 `key.interestOps(0)`，直接忽略这个 SelectionKey 的所有 I/O 事件。而当用户需要发送数据而调用了 `setSend()` 方法之后，会使得 nioSelector 开始监听 OP_WRITE 事件，在可以写数据到 SocketChannel 的时候调用 KafkaChannel.write() 方法将 KafkaChannel 内的 Send 对象写入到 SocketChannel 中去。那么 KafkaChannel 的 `write()` 方法到底是如何实现将 Send 对象数据写入 SocketChannel 的呢？

我们首先需要了解一下 `NetworkSend` 和 `NetworkReceive` 这两个类。在学习这两个类之前，我们需要了解一下 Java NIO 中的一个基本概念，SocketChannel 的 `read()` 和 `write()` 方法的参数都是一个 `ByteBuffer` 对象。read() 方法是从 SocketChannel 中读取字节内容放入参数的 ByteBuffer 对象中，而 write() 方法是将参数 ByteBuffer 对象中的字节内容写入 SocketChannel。并且需要注意的是 SocketChannel 实现了 `ScatteringByteChannel` 和 `GatheringByteChannel` 接口，read() 和 write() 方法可以接受多个 ByteBuffer 对象作为参数从而实现多个 ByteBuffer 的顺序读写。在这里 NetworkSend 和 NetworkReceive 使用了这个特性。

我们首先来了解一下 `NetworkSend` 类。

~~~java
/**
 * A size delimited Send that consists of a 4 byte network-ordered size N followed by N bytes of content
 */
// NetworkSend 同 NetworkReceive 一样, 包含了 size 和 content 两部分 ByteBuffer, 而 content 是一个 ByteBuffer 数组
public class NetworkSend extends ByteBufferSend {

    public NetworkSend(String destination, ByteBuffer buffer) {
        super(destination, sizeDelimit(buffer));
    }

    // 传入需要发送的 ByteBuffer, 分别构造 size buffer 和 content buffer 并作为数组返回
    // 先后关系很重要, 因为后面会将 ByteBuffer[] 集中写入 GatheringByteChannel
    private static ByteBuffer[] sizeDelimit(ByteBuffer buffer) {
        return new ByteBuffer[] {sizeBuffer(buffer.remaining()), buffer};
    }

    private static ByteBuffer sizeBuffer(int size) {
        ByteBuffer sizeBuffer = ByteBuffer.allocate(4);
        sizeBuffer.putInt(size);
        sizeBuffer.rewind();
        return sizeBuffer;
    }
}
~~~

我们再来看一下 NetworkSend 的父类 `ByteBufferSend`。

~~~java
public class ByteBufferSend implements Send {

    private final String destination;
    private final int size;
    protected final ByteBuffer[] buffers;
    private int remaining;
    private boolean pending = false;

    public ByteBufferSend(String destination, ByteBuffer... buffers) {
        this.destination = destination;
        this.buffers = buffers;
        for (ByteBuffer buffer : buffers)
            remaining += buffer.remaining();
        this.size = remaining;
    }

    @Override
    public String destination() {
        return destination;
    }

    @Override
    public boolean completed() {
        return remaining <= 0 && !pending;
    }

    @Override
    public long size() {
        return this.size;
    }

    @Override
    // 将 ByteBufferSend 发送到 SocketChannel, 返回成功写入的字节数
    public long writeTo(GatheringByteChannel channel) throws IOException {
        // 将 send 的多个 ByteBuffer 中数据一起写到 SocketChannel 中
        long written = channel.write(buffers);
        if (written < 0)
            throw new EOFException("Wrote negative bytes to channel. This shouldn't happen.");
        remaining -= written;
        pending = TransportLayers.hasPendingWrites(channel);
        return written;
    }
}
~~~

`NetworkSend` 类本质上是对 ByteBuffer 的抽象。从 `NetworkSend` 类的代码我们可以看到，它的构造方法的参数是一个 ByteBuffer 对象，就是这个 NetworkSend 的内容。当它的构造方法被调用时，会将 destination 信息和一个 ByteBuffer[] 对象传给父类构造函数，这个 ByteBuffer[] 实际上总是由 2 个 ByteBuffer 对象组成：
 + 第一个 ByteBuffer 对象是一个固定为 4 字节的 ByteBuffer，记录内容的长度。
 + 第二个 ByteBuffer 对象则是传入的参数，表示的是具体的内容。

当调用 `KafkaChannel.write()` 方法时，其实是将 `KafkaChannel.send` 字段（一个NetworkSend 对象）中的两个 ByteBuffer 中的内容依次写入 SocketChannel，这样我们就很容易理解了。下面是 KafkaChannel 类的 write() 方法的代码，它会试图发送自己的 send 字段，之后进入了 `ByteBufferSend.writeTo()` 方法：

~~~java
    // KafkaChannel 的 write() 方法。发送 Send 的核心方法, 如果没有完全发送完则返回 null, 发送完成则返回 Send 对象
    public Send write() throws IOException {
        Send result = null;
        if (send != null && send(send)) {
            result = send;
            send = null;
        }
        return result;
    }
    
    private boolean send(Send send) throws IOException {
        // 如果 send 在一次 write() 调用时没有发送完, SelectionKey 的 OP_WRITE 事件没有取消
        // 就会继续监听此 Channel 的 OP_WRITE 事件直到整个 send 发送完成
        send.writeTo(transportLayer);
        // 如果 send 发送完成则不再关注这个 SocketChannel 的 OP_WRITE 事件
        // 判断发送是否完成是通过查看 ByteBuffer 中是否还有剩余字节来判断的
        if (send.completed())
            transportLayer.removeInterestOps(SelectionKey.OP_WRITE);

        return send.completed();
    }
~~~

下面的代码是 `ByteBufferSend.writeTo()` 方法，其中 buffers 变量就是之前我们所说的 NetworkSend 类中的两个 ByteBuffer 对象了，分别表示内容的长度和具体内容。这样我们就知道了，Kafka 中发送的每一个信息都是由两部分组成：头部的 4 字节记录信息内容的长度，剩下的部分记录具体的信息内容。

~~~java
    // 将 ByteBufferSend 发送到 SocketChannel, 返回成功写入的字节数
    public long writeTo(GatheringByteChannel channel) throws IOException {
        // 将 send 的多个 ByteBuffer 中数据一起写到 SocketChannel 中
        long written = channel.write(buffers);
        if (written < 0)
            throw new EOFException("Wrote negative bytes to channel. This shouldn't happen.");
        remaining -= written;
        pending = TransportLayers.hasPendingWrites(channel);
        return written;
    }
~~~

## read() 方法分析
`read()` 方法用于 OP_READ 事件触发后，KafkaChannel 对象从 SocketChannel 中读取数据（服务器端，客户端都可以使用）。我们可以从下面 `NetworkReceive` 类的代码中先了解它的主要变量。可以看到，NetworkReceive 的成员变量非常简单，最重要的是两个 ByteBuffer 对象：`buffer` 和 `size`。经过对 `NetworkSend` 类的学习，我们可以推断，这个 size 变量就是记录 buffer 变量的长度（固定为 4 字节），而 buffer
变量则是信息的具体内容。

~~~java
public class NetworkReceive implements Receive {

    public final static String UNKNOWN_SOURCE = "";
    public final static int UNLIMITED = -1;

    private final String source;
    private final ByteBuffer size;
    private final int maxSize;
    private ByteBuffer buffer;
    
    // 省略...
}
~~~

和 `NetworkSend` 类相同，本质上，`NetworkReceive` 也是对一个 ByteBuffer 的抽象。从 SocketChannel 读取数据到 NetworkReceive 的过程就是填充 NetworkReceive 对象的 size 和 buffer 这两个 ByteBuffer 的过程。

~~~java
    // 读取 NetworkReceive 的核心方法, 从 SocketChannel 读取一个 NetworkReceive 对象并返回
    public NetworkReceive read() throws IOException {
        NetworkReceive result = null;

        // 先看上一次 read() 时产生的 receive 是否为 null, 如果不是 null, 说明上一次并未读取一个完整 NetworkReceive 对象
        if (receive == null) {
            receive = new NetworkReceive(maxReceiveSize, id);
        }

        // 核心函数, 从 TransportLayer(Kafka 对 SocketChannel的封装) 中读取一个 NetworkReceive 对象
        receive(receive);
        // 如果读取完成则将读取到的 NetworkReceive 对象作为 result 返回
        // 如果读取未完成则直接返回 null, 下一次触发 OP_READ 事件时继续填充这个 NetworkReceive 对象并返回
        if (receive.complete()) {
            receive.payload().rewind();
            result = receive;
            receive = null;
        }
        return result;
    }
    
    // 从 transportLayer（SocketChannel的抽象类）读取数据到 NetworkReceive 对象中去
    private long receive(NetworkReceive receive) throws IOException {
        return receive.readFrom(transportLayer);
    }
~~~

这里则是调用了 `NetworkReceive.readFrom()` 方法，其核心是 `readFromReadableChannel()` 方法。这个方法用来从 SocketChannel 中读取一个 NetworkReceive 对象。我们来看看这个方法的步骤：
 + 先从 Channel 读取 4 字节放入 size 中, `size.getInt()` 得到具体内容的长度 L。
 + 在申请一个长度为 L 字节的 ByteBuffer 为 buffer。
 + 从 Channel 一直读取内容放入 buffer，直到填满 buffer，NetworkReceive 对象就被填充好了。

~~~java
    // 从 channel 中读取 NetworkReceive 对象
    public long readFrom(ScatteringByteChannel channel) throws IOException {
        return readFromReadableChannel(channel);
    }

    // Need a method to read from ReadableByteChannel because BlockingChannel requires read with timeout
    // See: http://stackoverflow.com/questions/2866557/timeout-for-socketchannel-doesnt-work
    // This can go away after we get rid of BlockingChannel
    // 从 Channel 中读取 NetworkReceive 的过程是
    @Deprecated
    public long readFromReadableChannel(ReadableByteChannel channel) throws IOException {
        int read = 0;
        // 这里的条件 size.hasRemaining() 用于这次调用是读取一个新的 NetworkReceive 还是继续上一次未完成的读取
        if (size.hasRemaining()) {
            // 从 channel 读取 size 数据, size 这个 ByteBuffer 已经强制为 4 bytes 大小
            int bytesRead = channel.read(size);
            if (bytesRead < 0)
                throw new EOFException();
            read += bytesRead;
            if (!size.hasRemaining()) {
                size.rewind();
                // 从 size ByteBuffer 中读取整数, 即为后面具体内容的长度
                int receiveSize = size.getInt();
                if (receiveSize < 0)
                    throw new InvalidReceiveException("Invalid receive (size = " + receiveSize + ")");
                // 如果从 size 读取出来的长度大于最大值则抛出异常
                if (maxSize != UNLIMITED && receiveSize > maxSize)
                    throw new InvalidReceiveException("Invalid receive (size = " + receiveSize + " larger than " + maxSize + ")");

                // 根据 size 得到的大小申请相应大小的  ByteBuffer 并继续从 channel 中读取具体内容
                this.buffer = ByteBuffer.allocate(receiveSize);
            }
        }
        // 继续从 channel 中读取内容放入 buffer 中
        if (buffer != null) {
            int bytesRead = channel.read(buffer);
            if (bytesRead < 0)
                throw new EOFException();
            read += bytesRead;
        }

        return read;
    }
~~~

到这里，Socket 中的接收缓冲区的内容就被读取到了 KafkaChannel.receive 字段上了。

## KSelector 总结
通过对 KSelector 及相关类的 5 个重要方法的分析，我们大概理解了 Kafka 是如何对原生 Java NIO 的几个重要函数进行封装的。总体过程如下：
 + 客户端：
   + 客户端调用客户端的 KSelector 的 connect() 方法发起连接。
   + 客户端 KSelector 的 nioSelector 收到 OP_CONNECT 事件，调用 finishConnect() 方法建立连接并监控 OP_READ 事件。

 + 服务器端：
   + Acceptor 的 nioSelector 收到 OP_ACCEPT 事件，调用 accept() 方法建立连接。






