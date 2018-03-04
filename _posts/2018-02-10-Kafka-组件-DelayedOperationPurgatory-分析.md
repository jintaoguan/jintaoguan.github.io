---
layout:     post
title:      Kafka 组件 SystemTimer 及多层级 TimingWheel 分析
subtitle:   Kafka 延时操作管理
date:       2018-02-03
author:     Jintao
header-img: img/post-bg-universe.jpg
catalog: true
tags:
    - kafka
---

## 简介
由于 Kafka 这样的分布式系统请求量大，性能要求高，而 JDK 提供的 `DelayedQueue` 组件在底层使用 `堆` 数据结构，时间复杂度为 `O(log(n))`，所以 Kafka 为了将定时任务的存取操作和取消操作的时间复杂度降至 `O(1)`，实现了时间轮以达到性能要求，不过它的底层还是基于 Java 的 `DelayedQueue` 实现的。在这片文章中，我们将介绍 Timer 的相关功能及其实现。

## Timer 和 TimingWheel

我们先来看一下 `kafka.utils.timer.Timer` 接口的 API。Timer 接口的定义非常简单，为以下 4 个方法：添加任务，推进时钟并执行到期的任务，关闭 Timer 服务，以及查看当前剩余任务的数量。该接口只有一个 `kafka.utils.timer.SystemTimer` 实现，所以我们下面主要分析 SystemTimer 的代码。

~~~scala
trait Timer {

  // 向 Timer 中添加一个新的任务 TimerTask，当延时到期时执行
  
  def add(timerTask: TimerTask): Unit

  // 推进 Timer 的内部时钟并且执行到期的任务
  
  def advanceClock(timeoutMs: Long): Boolean

  // Timer 中尚未执行的任务数量
  
  def size: Int

  // 关闭 Timer, 忽略未执行的任务
  
  def shutdown(): Unit
}
~~~	

我们先来看一下 kafka 是如何使用 Timer 来接收并执行延时任务的。`DelayedOperationPurgatory` 的 `tryCompleteElseWatch()` 方法负责向 Timer 中不断添加延时任务，而外部线程 `ExpiredOperationReaper` 则无限循环调用 `Timer.advanceClock(200L)` 来推进 Timer 的内部时钟并同时执行延时任务。

首先我们需要知道的是 Timer 是面向调用者的接口，其背后由 `TimeWheel` 支持。TimingWheel（我们称其为时间轮）是一个存储定时任务的环形队列，底层使用数组实现，数组中的每个元素是一个 `TimerTaskList` 对象，我们称其为 `bucket`，因为它是包含多个延时任务的一个集合。`TimerTaskList` 是环形双向链表，在其中的链表项 `TimerTaskEntry` 中封装了真正的定时任务 `TimerTask`。TimerTaskList 中的 `expiration` 字段记录了整个 TimerTaskList 的到期时间戳，而 TimerTaskEntry 中的 `expirationMs` 字段记录了该任务的到期时间戳，`timerTask` 字段是其对应的 TimerTask 任务。值得注意的是，`TimerTaskList.expiration` 和其包含的所有的 TimerTaskEntry 的 `TimerTaskEntry.expirationMs` 不一定相同，后面做具体分析。TimerTaskList，TimerTaskEntry，TimerTask 这三个对象是 TimingWheel 实现的基础。

TimingWheel 提供了层级时间轮的概念，最底层时间轮的时间粒度最小，时间跨度也最小。每个层级的 TimingWheel 持有更高一层 TimingWheel 的引用（如果更高层时间轮存在的话）。`SystemTimer` 中持有最底层时间轮的引用（该时间轮时间格粒度为 1ms）。TimingWheel 中存在 `tickMs`，`wheelSize` 和 `internal` 三个重要的概念：tickMs 表示当前层级时间轮中一个时间格的时间跨度（由毫秒表示），wheelSize 记录了当前时间轮的格数，也就是 `buckets` 数组的大小，interval 是当前时间轮的时间跨度即 `tickMs * wheelSize`。不同层级时间轮使用相同的 wheelSize，且高一层时间轮的 tickMs 就是低一层时间轮的 interval。

## Timer 添加延时任务
我们可以先来分析一下通过 `SystemTimer.add()` 方法加入一个延时任务的过程。
* 将 `TimerTask` 封装成 `TimerTaskEntry` 对象。
* 尝试调用 `TimingWheel.add()` 方法添加 TimerTaskEntry 对象到 TimingWheel 中去。
* 如果任务没有被取消并且已经到期，则将任务提交给 `SytemTimer.taskExecutor` 执行。

~~~scala  
  def add(timerTask: TimerTask): Unit = {
    readLock.lock()
    try {
      // 计算任务到期时间, 并将 TimerTask 封装成 TimerTaskEntry
      
      addTimerTaskEntry(new TimerTaskEntry(timerTask, timerTask.delayMs + Time.SYSTEM.hiResClockMs))
    } finally {
      readLock.unlock()
    }
  }

  // 向 TimingWheel 中加入一个 TimerTaskEntry, 如果该 TimerTaskEntry 已经到期且未取消，则立刻执行
  
  private def addTimerTaskEntry(timerTaskEntry: TimerTaskEntry): Unit = {
    if (!timingWheel.add(timerTaskEntry)) {
      // Already expired or cancelled
      
      if (!timerTaskEntry.cancelled)
        taskExecutor.submit(timerTaskEntry.timerTask)
    }
  }
~~~

我们再来一看看 `TimingWheel.add()` 方法的执行步骤：
* 取得任务的到期时间戳，如果该任务已经被取消则直接返回。
* 如果 `expiration < currentTime + tickMs` 表示该任务已经到期，直接返回。
* 如果 `expiration >= currentTime + tickMs` 并且 `expiration < currentTime + interval` 表示该任务尚未到期，并且位于当前时间轮的时间范围内，将其添加进其对应的 bucket（即 TimerTaskList）中，并更新该 TimerTaskList 的 expiration 字段（也是绝对时间戳）为该 bucket 的起始时间戳。如果 `TimerTaskList.setExpiration()` 方法返回 true，即表示这个 TimerTaskList 覆盖了原来的时间格（因为是环形数组），那么我们将这个 TimerTaskList 加入 DelayedQueue。如果返回 false，则表示该 TimerTaskList 之前已经被添加进 DelayedQueue 中。
* 如果 `expiration >= currentTime + interval`，则表示该延时任务的到期时间已经超过了当前时间轮的时间范围，我们需要将其添加进更高层时间轮（`overflowWheel`），因为更高层时间轮覆盖的时间范围更大。如果当前时间轮没有更高层时间轮，则先调用 `addOverflowWheel()` 方法添加更高层时间轮，再将该任务添加进高层时间轮。


~~~scala
  def add(timerTaskEntry: TimerTaskEntry): Boolean = {
    // 这里的 expirationMs 是绝对到期时间, 即到期时间戳
    
    val expiration = timerTaskEntry.expirationMs

    if (timerTaskEntry.cancelled) {
      // Cancelled
      
      // 任务已经被取消
      
      false
    } else if (expiration < currentTime + tickMs) {
      // Already expired
      
      // 任务已经到期
      
      false
    } else if (expiration < currentTime + interval) {
      // Put in its own bucket
      
      // 任务在当前级 TimingWheel 的时间跨度范围内
      
      // 按照任务的到期时间查找此任务属于的时间格
      
      val virtualId = expiration / tickMs
      val bucket = buckets((virtualId % wheelSize.toLong).toInt)
      // 将 TimerTaskEntry 加入到对应的 bucket 中, 即加入 TimerTaskList
      
      bucket.add(timerTaskEntry)

      // Set the bucket expiration time
      
      // 更新该 TimerTaskList 对应的到期时间戳
      
      // 如果更新成功，则表示这个 bucket 覆盖了之前的时间格（因为是环形数组），需要添加进 DelayedQueue 中
      
      if (bucket.setExpiration(virtualId * tickMs)) {
        // The bucket needs to be enqueued because it was an expired bucket
        
        // We only need to enqueue the bucket when its expiration time has changed, i.e. the wheel has advanced
        
        // and the previous buckets gets reused; further calls to set the expiration within the same wheel cycle
        
        // will pass in the same value and hence return false, thus the bucket with the same expiration will not
        
        // be enqueued multiple times.
        
        queue.offer(bucket)
      }
      true
    } else {
      // Out of the interval. Put it into the parent timer
      
      // 任务不在当前级 TimingWheel 的时间跨度范围内, 则放入上一级 TimingWheel 中
      
      if (overflowWheel == null) addOverflowWheel()
      overflowWheel.add(timerTaskEntry)
    }
  }
~~~

向 SystemTimer 以及 TimingWheel 中添加延时任务的过程并不复杂，我们只需要知道这几个重要的概念。

* 时间轮有层级的概念，低级时间轮覆盖范围小，高级时间轮覆盖范围大，SystemTimer 首先使用最底层时间轮，其单个时间格是 1ms。如果添加的延时任务太远，不在当前层级时间轮的时间范围内，则将其添加进更高层时间轮中。这是一个“由下向上”查询的过程。
* 当向 SystemTimer 中添加任务时：
  * 如果任务取消，则直接忽略。
  * 如果任务未取消但是已经到期，则立刻将任务交由 `SystemTimer.taskExecutor` 线程池执行。
  * 如果任务未取消并且未到期，则尝试将该任务添加进最底层时间轮（或者更高级时间轮）。
* 更新 bucket 的到期时间时，


## Timer 执行延迟任务
添加任务比较容易理解，但是延时任务是如何被 Timer 执行的呢？这是因为外部线程 `ExpiredOperationReaper` 不断调用 `Timer.advanceClock(200L)` 从 `DelayedQueue` 中取出到期任务，并推进 TimingWheel 的内部表针 `currentTime` 变量来维护 TimingWheel 中的延时任务。

我们可以从 `SystemTimer.advanceClock(timeoutMs)` 方法开始分析整个过程。
* 从整个多层级 TimingWheel 共用的 DelayedQueue 中取出已到期的 bucket 即一个 TimerTaskList 对象。值得注意的是这里调用的方法是 `DelayedQueue.poll(timeoutMs, TimeUnit.MILLISECONDS)`，这是一个阻塞方法 - 如果有已经到期的元素，则立刻返回该元素；如果没有到期的元素，则至多等待 timeoutMs 毫秒，等待过程中如果有到期元素则立刻返回该到期元素；如果等待了 timeoutMs 毫秒后仍然没有到期的元素，则返回 null。
* 此时判断取出的 bucket 是否为 null。如果 `bucket == null`，表示当前没有到期任务，则跳过此次推进，即使已经阻塞了 timeoutMs 毫秒，也不在这里更新各层级 TimeWheel 的内部表针 currentTime 变量。
* 如果 `bucket != null`，表示当前有到期的 bucket 即 TimerTaskList 对象。但是我们知道 TimerTaskList 是多个 TimerTaskEntry 对象组成的双向链表。属于同一个 TimerTaskList 的所有 TimerTaskEntry 都处于同一个时间格内（时间格由当前的时间轮决定），但是不一定都是相同的到期时间戳。
* 对于这个到期的 bucket，我们可以得到它的到期时间戳 `bucket.getExpiration()`。这个到期时间戳是 TimerTaskList 的到期时间戳，表示 bucket 时间格覆盖范围的起始时刻，和具体任务的到期时间戳不一定相同，但是这个 TimerTaskList 所包含的所有 TimerTaskEntry 的到期时间戳一定在这个 TimerTaskList 对应的 bucket 的覆盖范围以内。将各层级时间轮的内部表针推进到这个 bucket 的起始时刻，即 bucket.getExpiration()。
* 核心步骤。`bucket.flush(reinsert)` 方法对这个 TimerTaskList 对象中所包含的所有 TimerTaskEntry 重新检测，在这个过程中，到期任务会被提交给 `SystemTimer.taskExecutor` 执行，未到期的任务则会被重新添加进时间轮。为什么要重新添加呢？因为这个 bucket 可能原来位于高层时间轮，而重新加入时间轮后就会被放入低层级时间轮。这一步完成了延时任务的执行，也会将 bucket 中未到期的任务重新放入对应的时间轮。
* 最后一步 `bucket = delayQueue.poll()` 比较令人迷惑，为什么要循环执行一遍呢？我能想到的原因是，在以上几个步骤之后，已经有可能过去了一些时间，可能有一些在方法开始时还没有到期的 bucket 在这里刚刚到期了。所以我们再从 DelayedQueue 中取出目前已经到期的 bucket 再检测一遍，直到当前时刻没有任何到期的 bucket。

~~~scala
  def advanceClock(timeoutMs: Long): Boolean = {
    // 从 delayQueue 中获取到期的 bucket 即 TimerTaskList
    
    // 如果没有到期的 bucket 则最多阻塞等待 timeoutMs 再返回
    
    var bucket = delayQueue.poll(timeoutMs, TimeUnit.MILLISECONDS)
    if (bucket != null) {
      writeLock.lock()
      try {
        // 核心步骤
        
        while (bucket != null) {
          timingWheel.advanceClock(bucket.getExpiration())
          bucket.flush(reinsert)
          bucket = delayQueue.poll()
        }
      } finally {
        writeLock.unlock()
      }
      true
    } else {
      false
    }
  }
~~~

`TimingWheel. advanceClock(timeMs)` 方法仅仅负责更新时间轮的内部表针 `currentTime` 变量，而和具体的延时任务的执行没有关系。它的作用很简单，就是更新内部表针 `currentTime` 变量到 `timeMs` 对应的时间格的起始时刻，再递归调用，尝试推进更高级时间轮的内部表针。

* 如果时间戳 timeMs 大于等于下一个 bucket 的到期时间，则允许这次推进，否则直接退出。
* 推进当前时间轮的表针 currentTime 到 timeMs 对应的时间格的起始时刻。有可能跳过一个或者多个 bucket。
* 同时也类似地推进上层的时间轮的表针。

~~~scala  
  def advanceClock(timeMs: Long): Unit = {
    // 如果时间戳 timeMs 大于等于下一个 bucket 的到期时间，则允许这次推进
    
    if (timeMs >= currentTime + tickMs) {
    
      // 然后推进 currentTime 到 timeMs 对应的时间格的起始时刻，有可能跳过一个或者多个 bucket
      currentTime = timeMs - (timeMs % tickMs)

      // Try to advance the clock of the overflow wheel if present
      
      // 推进更高层的 TimingWheel
      
      if (overflowWheel != null) overflowWheel.advanceClock(currentTime)
    }
  }
~~~

再来分析一下 `bucket.flush(reinsert)` 调用。`TimerTaskList.flush()` 遍历 TimerTaskList 中所有的 TimerTaskEntry 对象，并对每个 TimerTaskEntry 对象调用传入的 `f` 方法。该方法使用了 `f: (TimerTaskEntry)=>Unit` 作为其参数，而 `reinsert` 函数就是传入的参数。这是 scala 中一个的典型的**函数作为参数传递给一个方法**的例子。

~~~scala
  // Remove all task entries and apply the supplied function to each of them
  
  def flush(f: (TimerTaskEntry)=>Unit): Unit = {
    synchronized {
      var head = root.next
      while (head ne root) {
        remove(head)
        f(head)
        head = root.next
      }
      expiration.set(-1L)
    }
  }
~~~

`reinsert` 是一个**高阶函数**。它的代码如下：
~~~scala
  private[this] val reinsert = (timerTaskEntry: TimerTaskEntry) => addTimerTaskEntry(timerTaskEntry)
~~~

理解了以上代码，我们就能够知道，`bucket.flush(reinsert)` 的意思是对 bucket 即 TimerTaskList 中的每个 TimerTaskEntry 对象调用 `addTimerTaskEntry(timerTaskEntry)` 方法。而我们之前已经对 `addTimerTaskEntry(timerTaskEntry)`  方法做过分析 - 如果 timerTaskEntry 已到期，则执行；如果未到期，则加入时间轮。所以 `bucket.flush(reinsert)` 调用负责到期任务的执行。

## 多层级 TimingWheel 示例
我们可以通过下面的一个示例来理解多层级 TimingWheel 执行延时任务的运行过程。假设现在我们的 SystemTimer 内部已经拥有了两个时间轮：Level 0 时间轮 `TW0` 以及 Level 1 时间轮 `TW1`。
* TW0 时间轮的时间格 tickMs 为 1ms，数组长度 wheelSize 为 8，时间跨度 interval 为 1ms * 8 = 8ms
* TW1 时间轮的时间格 tickMs 为 8ms，数组长度 wheelSize 为 8，时间跨度 interval 为 8ms * 8 = 64ms

![](/img/post-img/2018-03-02/timingwheel.png)

1）在 Time 0 时刻，TW1 的内部表针 currentTime 变量指向的是 0ms，而 TW2 的内部表针 currentTime 变量指向的也是 0ms。我们在这个时刻从 Timer 加入三个延时任务 `T7`，`T8`，`T9`，三个任务分别延时 7ms，8ms，9ms 后执行。所以我们看到，T7 的延时属于低级时间轮 TW0 的覆盖范围内（0ms - 7ms），所以落入了 TW0。而 T8，T9 的延时超过了 TW0 的覆盖范围，所以被加入了高级时间轮 TW1。那么在各层级时间轮共享的 DelayedQueue 中，我们加入了两个 TimerTaskList 对象。第一个 TimerTaskList 对象的 expiration 字段是 7ms，只包含任务 T7。第二个 TimerTaskList 对象的 expiration 字段是 8ms，包含了任务 T8 和 T9。因为第二个 TimerTaskList 对象被加进了 TW1，所以其 expiration 字段是根据 TW1 时间格来的。

2）`ExpiredOperationReaper` 调用 `Timer.advanceClock(200L)` 方法，该方法先调用 `delayQueue.poll(timeoutMs, TimeUnit.MILLISECONDS)`，它会在阻塞 7ms 后，从 DelayedQueue 取得到期的 TimerTaskList 对象，即 T7 任务所对应的 TimerTaskList。这时就会推进各层级时间轮内部表针到该 TimerTaskList 对应的到期时间戳：先推进 TW0 的 currentTime 到 7ms，然后再推进 TW1 的 currentTime，因为 TW1 的单个时间格跨度为 8ms，所以 TW1 内部表针无法推进。这时 TW0 的覆盖事件跨度是 `[7ms, 15ms)`，TW1 的覆盖时间跨度是 `[0ms, 64ms)`。之后进入 `bucket.flush(reinsert)`，将取出的 TimerTaskList 中所有 TimerTaskEntry 重新加入最底层时间轮 TW0。但是 T7 此时已经到期，就被 SystemTimer 提交到 `SystemTimer.taskExecutor` 线程池执行了。这时图中就是 Time 7 时刻的情况。

3）`ExpiredOperationReaper` 再一次调用 `Timer.advanceClock(200L)` 方法，DelayedQueue 会返回第二个 TimerTaskList 即包含 T8，T9 任务的 TimerTaskList。同样地，先推进各层级时间轮。TW0 的内部表针 currentTime 会变成 8ms，而 TW1 的内部表针 currentTime 也会变成 8ms。这时 TW0 的覆盖事件跨度是 `[8ms, 16ms)`，TW1 的覆盖时间跨度是 `[8ms, 72ms)`。之后调用 `bucket.flush(reinsert)` 重新添加 TimerTaskList 包含的所有延时任务。T8 的 expirationMs 是 8ms，满足 `expirationMs < currentTime + tickMs`，所以已经到期，交给 `SystemTimer.taskExecutor` 线程池执行。而 T9 的 expirationMs 是 9ms，满足条件 `currentTime + tickMs <= expiration < currentTime + interval`，所以它被添加进 Level 0 时间轮 TW0 和 DealyedQueue 中。这时 DelayedQueue 中只有一个 bucket 了，就是 只包含 T9 任务的 TimerTaskList 对象。而 T9 这时从 TW1 被转移到了 TW0 时间轮中。TW0 中只保存了 T9。如图中 Time 8 时刻所示。

## 性能比较
看完代码，自然会产生一些疑问。Kafka 的 `SystemTimer` 同样使用了 `java.util.concurrent.DelayedQueue` 来存储延时任务，为什么还需要多层级时间轮呢？使用多层级时间轮的意义是什么呢？为什么不直接将所有延时任务直接放入 DelayedQueue 然后不断调用 `DelayedQueue.poll()` 取出到期任务再执行呢？这里有两个原因：

* 因为 DelayedQueue 是由 `堆` 实现的，添加/删除操作的时间复杂度为 `O(log(n))`，如果将所有延时任务不经过多层级时间轮，全部放入 DelayedQueue，当任务数量很多的时候，DelayedQueue 中存储的任务很多，添加/删除操作就会很慢。而使用了多层级 TimingWheel 之后，虽然在底层时间轮上的任务仍然为一个时刻一个 bucket，但是延时比较久（即放入高层时间轮）的任务即使到期时刻不一样，到期时刻处于相同时间格的延时任务仍然会被放入同一个 bucket 即 TimerTaskList 内，再放入 DelayedQueue。这样处理就大大减少了 DelayedQueue 中的元素数量，那么也就加快了 DelayedQueue 的添加/删除操作的速度。

* 更重要的是，对于添加任务的操作，如果该任务在时间轮的对应时间格上已经有了有效 TimerTaskList 对象（即该 TimerTaskList 未到期并且已经放入了 DelayedQueue），那么就直接将任务加入该 TimerTaskList 即可，不需要重新将 TimerTaskList 加入 DelayedQueue，这样的添加操作的时间复杂度就是 O(1)，这样大大加速了添加延时任务的速度。


参考文章：
* [Apache Kafka, Purgatory, and Hierarchical Timing Wheels](https://www.confluent.io/blog/apache-kafka-purgatory-hierarchical-timing-wheels/)
* [惊艳的时间轮定时器](http://www.cnblogs.com/zhongwencool/p/timing_wheel.html)








