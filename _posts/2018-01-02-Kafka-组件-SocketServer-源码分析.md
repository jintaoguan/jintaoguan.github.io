---
layout:     post
title:      Kafka 组件 SocketServer 分析
subtitle:   Kafka 网络通信基础
date:       2018-01-02
author:     Jintao
header-img: img/post-bg-universe.jpg
catalog: true
tags:
    - kafka
    - distributed system
---

## 概述
SocketServer 是对一个 broker 的相关 ServerSocket 的抽象，用于管理这个 broker 的底层 socket 连接与网络通信。 broker 所有的网络通信都由其 SocketServer 对象管理和处理。Kafka SocketServer 是基于 Java NIO 来开发的，采用了 Reactor 模式，其中包含了1个 Acceptor 负责接受客户端请求，N个 Processor 负责读写数据，M个 Handler 来处理业务逻辑。

每个 Acceptor 对象拥有一个 NSelector 对绑定的 ServerSocketChannel 监听 OP_ACCEPT 消息。
每个 Processor 都有一个 KSelector，用来监听多个客户端 SocketChannel，因此可以非阻塞地处理多个客户端的读写请求。

每个 Processor 中都有一个 newConnections 队列来缓存客户端连接，负责 Accepter 与 Processor 间的通信。
所有的 Processor 共用 RequestChannel 的 requestQueue 队列来缓存所有客户端的 request。当 Processor 处理完 request，将该 request 的 response 缓存进 responseQueues 队列(每个 Processor 有自己单独的一个队列)

![](/img/request_lifecycle.png)

所以 Kafka 的 SocketServer 是一个典型的 SEDA (Staged Event-Driven Architecture) 架构。

## SocketServer 的启动
当 broker 启动时，KafkaServer.startup() 方法中会调用 SocketServer.startup() 方法。SocketServer 启动时，
 + 首先为每一个 Acceptor（实际上总共只有一个 Acceptor）创建 numProcessorThreads 个 Processor 线程，并全部放入 processors 数组用于索引（此时所有 Processor 线程并没有启动）。
 + 然后创建 Acceptor，关联其对应的 processor 线程并启动这个 Acceptor 线程。

~~~scala
  /**
   * Start the socket server
   */
  // kafka 启动时会通过 KafkaServer 对象调用 startup() 方法
  
  def startup() {
    this.synchronized {

      connectionQuotas = new ConnectionQuotas(maxConnectionsPerIp, maxConnectionsPerIpOverrides)

      // 发送数据的 buffer 大小
      
      val sendBufferSize = config.socketSendBufferBytes
      // 接收数据的 buffer 大小
      
      val recvBufferSize = config.socketReceiveBufferBytes
      // 本机的 brokerId
      
      val brokerId = config.brokerId

      var processorBeginIndex = 0
      config.listeners.foreach { endpoint =>
        val listenerName = endpoint.listenerName
        val securityProtocol = endpoint.securityProtocol
        val processorEndIndex = processorBeginIndex + numProcessorThreads

        // 对于每个 endpoint, 根据 numProcessorThreads 创建多个 processor
        
        for (i <- processorBeginIndex until processorEndIndex)
          processors(i) = newProcessor(i, connectionQuotas, listenerName, securityProtocol)

        // 对于这个 endpoint 创建其对应的 Acceptor (关联其对应的 processor 线程)
        
        val acceptor = new Acceptor(endpoint, sendBufferSize, recvBufferSize, brokerId,
          processors.slice(processorBeginIndex, processorEndIndex), connectionQuotas)
        acceptors.put(endpoint, acceptor)
        // 创建并启动这个 Acceptor 线程
        
        Utils.newThread(s"kafka-socket-acceptor-$listenerName-$securityProtocol-${endpoint.port}", acceptor, false).start()
        // 通过 Acceptor 中的 latch 阻塞住 startup() 线程, 直到 Acceptor 完全启动
        
        acceptor.awaitStartup()

        processorBeginIndex = processorEndIndex
      }
    }
~~~


这是 Acceptor 的构造函数部分。
 + 创建一个 Java NIO Selector 对象。
 + 创建一个 ServerSocketChannel 对象, 绑定该 endpoint 的 host 和 port。
 + 创建并启动这个 Acceptor 所对应的所有 Processor 线程。
 
~~~scala
  // 每个 Acceptor 拥有一个 NIO Selector 对象, 对应一个 ServerSocketChannel 对象
  
  private val nioSelector = NSelector.open()
  val serverChannel = openServerSocket(endPoint.host, endPoint.port)

  // 启动这个 Acceptor 管理的所有 processor 线程
  
  this.synchronized {
    processors.foreach { processor =>
      Utils.newThread(s"kafka-network-thread-$brokerId-${endPoint.listenerName}-${endPoint.securityProtocol}-${processor.id}",
        processor, false).start()
    }
  }
~~~

## Acceptor 线程的运行
然后我们再来看 Acceptor 的 run() 方法，它是 Acceptor 的核心方法，用于监听和接受客户端的连接。
 + 首先将 Acceptor 对应的 ServerSocketChannel 对象注册到 Java NIO Selector 上并监听其 OP_ACCEPT 事件。当有新的客户端连接该 ServerSocket 时，会触发 Java NIO Selector 的 OP_ACCEPT 事件。
 + 进入工作周期，Acceptor 进行循环调用 nioSelector.select() 与 nioSelector.selectedKeys() 来处理 selector 触发的新事件。
 + 当 key.isAcceptable == true 时（可以接受客户端的连接请求），调用 accept(key: SelectionKey, processor: Processor)，以 round robin 轮询的方式让一个 Processor 线程接受并处理这个连接。
 + 至此，Acceptor 的一次工作周期就结束了。Acceptor 继续循环查看 nioSelector 是否有新的 OP_ACCEPT 事件发生。

~~~scala
  /**
   * Accept loop that checks for new connection attempts
   */
  // Acceptor 利用 Java NIO 的 selector 来接受客户端网络连接.
  
  def run() {
    // 在 selector 注册 SelectionKey.OP_ACCEPT 事件, SelectionKey 是表示一个 Channel 和 Selector 的注册关系。
    
    // 在 Acceptor 中的 selector, 只有监听客户端连接请求的 ServerSocketChannel 的 OP_ACCEPT 事件注册在上面。
    
    // 当 selector 的 select 方法返回时, 则表示注册在它上面的 Channel 发生了对应的事件。
    
    // 在 Acceptor 中, 这个事件就是 OP_ACCEPT, 表示这个 ServerSocketChannel 的 OP_ACCEPT 事件发生了.
    
    // 四种事件 1) OP_ACCEPT, 2) OP_CONNECT, 3) OP_READ, 4) OP_WRITE
    
    serverChannel.register(nioSelector, SelectionKey.OP_ACCEPT)
    startupComplete()
    try {
      var currentProcessor = 0
      while (isRunning) {
        try {
          // 开始等待客户端的连接请求
          
          // 在调用 select() 并返回了有 channel 就绪之后，可以通过选中的 key 集合来获取 channel.
          
          // 当 selector 的 select 方法返回时, 则表示注册在它上面的 Channel 发生了对应的事件.
          
          val ready = nioSelector.select(500)
          if (ready > 0) {
            // 可以通过选中的 key 集合来获取 channel
            
            val keys = nioSelector.selectedKeys()
            val iter = keys.iterator()
            while (iter.hasNext && isRunning) {
              try {
                val key = iter.next
                iter.remove()
                // a connection was accepted by a ServerSocketChannel
                
                if (key.isAcceptable)
                  // 接受一个新的网络连接, 通过 currentProcessor 索引到一个 processor 并由它处理
                  
                  accept(key, processors(currentProcessor))
                else
                  throw new IllegalStateException("Unrecognized key state for acceptor thread.")

                // round robin to the next processor thread
                
                // 通过 currentProcessor 索引切换到下一个 processor
                
                currentProcessor = (currentProcessor + 1) % processors.length
              } catch {
                case e: Throwable => error("Error while accepting connection", e)
              }
            }
          }
        }
        catch {
          // We catch all the throwables to prevent the acceptor thread from exiting on exceptions due

          // to a select operation on a specific channel or a bad request. We don't want
          
          // the broker to stop responding to requests from other clients in these scenarios.
          
          case e: ControlThrowable => throw e
          case e: Throwable => error("Error occurred", e)
        }
      }
    } finally {
      debug("Closing server socket and selector.")
      swallowError(serverChannel.close())
      swallowError(nioSelector.close())
      shutdownComplete()
    }
  }
~~~

我们来看 Acceptor 的 accept(key: SelectionKey, processor: Processor) 方法做了些什么。
 + 首先通过 SelectionKey 来拿到对应的 ServerSocketChannel。
 + 调用 ServerSocketChannel 的 accept() 方法来建立和客户端的连接，并获取客户端的 SocketChannel。
 + 配置客户端 SocketChannel 的连接，设置为长连接，非阻塞模式。
 + 调用 processor.accept(socketChannel) 将客户端的 SocketChannel 交给了 Processor 处理。

~~~scala
/*
   * Accept a new connection
   */
  // 接受一个网络连接, 并交给 processor 处理
  
  def accept(key: SelectionKey, processor: Processor) {
    // 获取 ServerSocketChannel 对象
    
    val serverSocketChannel = key.channel().asInstanceOf[ServerSocketChannel]
    // 从 ServerSocketChannel 对象获得客户端的 SocketChannel 对象
    
    val socketChannel = serverSocketChannel.accept()
    try {
      // 配置客户端 SocketChannel 的连接
      
      connectionQuotas.inc(socketChannel.socket().getInetAddress)
      // 设置为非阻塞模式
      
      socketChannel.configureBlocking(false)
      socketChannel.socket().setTcpNoDelay(true)
      // 设置为长连接
      
      socketChannel.socket().setKeepAlive(true)
      if (sendBufferSize != Selectable.USE_DEFAULT_BUFFER_SIZE)
        socketChannel.socket().setSendBufferSize(sendBufferSize)

      debug("Accepted connection from %s on %s and assigned it to processor %d, sendBufferSize [actual|requested]: [%d|%d] recvBufferSize [actual|requested]: [%d|%d]"
            .format(socketChannel.socket.getRemoteSocketAddress, socketChannel.socket.getLocalSocketAddress, processor.id,
                  socketChannel.socket.getSendBufferSize, sendBufferSize,
                  socketChannel.socket.getReceiveBufferSize, recvBufferSize))

      // 将客户端的 SocketChannel 交给 processor 处理
      
      processor.accept(socketChannel)
    } catch {
      case e: TooManyConnectionsException =>
        info("Rejected connection from %s, address already has the configured maximum of %d connections.".format(e.ip, e.count))
        close(socketChannel)
    }
  }
~~~

到这里工作流进入了 processor.accept(socketChannel) 方法。Processor 的 accept() 方法很简单，就是将新连接的 SocketChannel 保存到 Processor 自己的 newConnections 队列中。由于只是简单地将 SocketChannel 对象保存到自己的 newConnections 队列中, 所以每个 Processor 都会处理多个客户端的请求。

~~~scala
  /**
   * Queue up a new connection for reading
   */
  def accept(socketChannel: SocketChannel) {
    newConnections.add(socketChannel)
    // 唤醒 Processor 的 selector
    
    wakeup()
  }
~~~

## Processor 线程的运行
现在我们来看一看 Processor 线程的核心方法 run()。
 + 首先从 newConnection 队列里取出客户端 SocketChannel, 注册到该 Processor 的 KSelector 中, 监听该 channel 的 OP_READ 事件。
 + 读取客户端 SocketChannel 发送的 request，并构造 Kafka 内部网络层通用的 RequestChannel.Request 对象，再将这个 RequestChannel.Request 放入 RequestChannel 对象的 requestQueue 队列中，等待 KafkaRequestHandler 线程对其进行具体的业务操作处理。
 + 

~~~scala
  override def run() {
    startupComplete()
    while (isRunning) {
      try {
        // setup any new connections that have been queued up
        
        // 从 newConnection 队列里取出客户端 SocketChannel, 添加到自身的 KSelector中, 监听该 channel 的 OP_READ 事件
        
        configureNewConnections()
        // register any new responses for writing
        
        // 处理当前所有处理完成的 request 相应的response, 这些 response 都是从 RequestChannel 获得 (requestChannel.receiveResponse)
        
        // 根据 request 的类型来决定从当前连接的 nio selector 中暂时删除读事件监听/添加写事件/关闭当前连接
        
        processNewResponses()
        poll()
        processCompletedReceives()
        processCompletedSends()
        processDisconnected()
      } catch {
        // We catch all the throwables here to prevent the processor thread from exiting. We do this because
        
        // letting a processor exit might cause a bigger impact on the broker. Usually the exceptions thrown would
        
        // be either associated with a specific socket channel or a bad request. We just ignore the bad socket channel
        
        // or request. This behavior might need to be reviewed if we see an exception that need the entire broker to stop.
        
        case e: ControlThrowable => throw e
        case e: Throwable =>
          error("Processor got uncaught exception.", e)
      }
    }

    debug("Closing selector - processor " + id)
    swallowError(closeAll())
    shutdownComplete()
  }
~~~

Processor 的 configureNewConnections() 方法负责很关键的一步工作。
 + 从该 Processor 的 newConnections 队列中 poll 一个客户端 SocketChannel 对象。
 + 将这个客户端的 SocketChannel 的 注册到该 Processor 的 KSelector 上面并监听该 channel 的 OP_READ 事件。需要注意的是，这里的 KSelector 是 Acceptor 使用的原生 Java NIO Selector（NSelector） 的一个封装（希望以后能进一步分析）。Processor 的 KSelector 需要监听多个客户端 SocketChannel 的 OP_READ 事件，而 Acceptor 的 NSelector 只需要监听一个服务器端的 ServerSocketChannel 的 OP_ACCEPT 事件。
 
~~~scala
  /**
   * Register any new connections that have been queued up
   */
  // 如果有队列中有新的 SocketChannel, 则它首先将其 OP_READ 事情注册到该 Processor 的 KSelector上面, 监听 OP_READ 事件
  
  private def configureNewConnections() {
    while (!newConnections.isEmpty) {
      // 取得新连接的客户端 SocketChannel 对象
      
      val channel = newConnections.poll()
      try {
        debug(s"Processor $id listening to new connection from ${channel.socket.getRemoteSocketAddress}")
        val localHost = channel.socket().getLocalAddress.getHostAddress
        val localPort = channel.socket().getLocalPort
        val remoteHost = channel.socket().getInetAddress.getHostAddress
        val remotePort = channel.socket().getPort
        // [localHost: String, localPort: Int, remoteHost: String, remotePort: Int] 四元组组成 ConnectionId
        
        val connectionId = ConnectionId(localHost, localPort, remoteHost, remotePort).toString
        // 注册这个 connection 到 KSelector
        
        // 将这个客户端的 SocketChannel 的 注册到该 Processor 的 KSelector 上面并监听该 channel 的 OP_READ 事件

        selector.register(connectionId, channel)
      } catch {
        // We explicitly catch all non fatal exceptions and close the socket to avoid a socket leak. The other
        
        // throwables will be caught in processor and logged as uncaught exceptions.
        
        case NonFatal(e) =>
          val remoteAddress = channel.getRemoteAddress
          // need to close the channel here to avoid a socket leak.
          
          close(channel)
          error(s"Processor $id closed connection from $remoteAddress", e)
      }
    }
  }
~~~


~~~scala
  private def processCompletedReceives() {
    selector.completedReceives.asScala.foreach { receive =>
      try {
        val openChannel = selector.channel(receive.source)
        val session = {
          // Only methods that are safe to call on a disconnected channel should be invoked on 'channel'.
          
          val channel = if (openChannel != null) openChannel else selector.closingChannel(receive.source)
          RequestChannel.Session(new KafkaPrincipal(KafkaPrincipal.USER_TYPE, channel.principal.getName), channel.socketAddress)
        }
        // 这一步很重要, 构造 Kafka 内部网络层通用的 RequestChannel.Request 对象
        
        val req = RequestChannel.Request(processor = id, connectionId = receive.source, session = session,
          buffer = receive.payload, startTimeMs = time.milliseconds, listenerName = listenerName,
          securityProtocol = securityProtocol)
        // Processor 将 request 交给 requestChannel 的 requestQueue, 之后由 KafkaRequestHandler 与 KafkaApis 处理
        
        requestChannel.sendRequest(req)
        // 移除对 OP_READ 事件的监听. 接收本身就是 Read, 接收到响应后, 就不需要再读了
        
        selector.mute(receive.source)
      } catch {
        case e @ (_: InvalidRequestException | _: SchemaException) =>
          // note that even though we got an exception, we can assume that receive.source is valid.
          
          // Issues with constructing a valid receive object were handled earlier
          
          error(s"Closing socket for ${receive.source} because of error", e)
          close(selector, receive.source)
      }
    }
  }
~~~
