/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.util

import java.util.Collections

import scala.collection.JavaConverters._

import org.apache.commons.lang3.SystemUtils
import org.slf4j.Logger
import sun.misc.{Signal, SignalHandler}

import org.apache.spark.internal.Logging

/**
 * Contains utilities for working with posix signals.
 */
private[spark] object SignalUtils extends Logging {

  /** A flag to make sure we only register the logger once. */
  private var loggerRegistered = false

  /** 
   * Register a signal handler to log signals on UNIX-like systems. 
   * 
   * synchronized 关键字解释：
   * 1. 保证同一时刻 synchronized 关键字中的代码只被执行一次。
   * 2. 若两个线程同时需要执行，那么其中一个线程会等待另外一个线程执行完成后，再执行代码块中的代码。
   *
   * TERM: 终止信号
   * HUP: 终端挂起或者控制进程终止
   * INT: 键盘中断
   *
   * sun.misc.Signal 使用说明：http://www.programgo.com/article/58772663037/
   * 
   * 
   * 
   */
  def registerLogger(log: Logger): Unit = synchronized {
    if (!loggerRegistered) {
      Seq("TERM", "HUP", "INT").foreach { sig =>
        // 下面代码等于
        // 1. 先执行: log.error("RECEIVED SIGNAL " + sig)
        // 2. 再执行: SignalUtils.register(sig)(false)
        SignalUtils.register(sig) {
          log.error("RECEIVED SIGNAL " + sig)
          false
        }
      }
      loggerRegistered = true
    }
  }

  /**
   * Adds an action to be run when a given signal is received by this process.
   *
   * Note that signals are only supported on unix-like operating systems and work on a best-effort
   * basis: if a signal is not available or cannot be intercepted, only a warning is emitted.
   *
   * All actions for a given signal are run in a separate thread.
   */
  def register(signal: String)(action: => Boolean): Unit = synchronized {
    if (SystemUtils.IS_OS_UNIX) {
      try {
        // handlers 是一个可变的HashMap[String, ActionHandler]。
        // getOrElseUpdate 说明：
        // - 1. 在一个 可变的 var hm = cala.collection.mutable.HashMap [k: String, f(k): Funciton]中，执行 hm(key)。
        // - 2. 若 hm(k) 的键存在，则不再执行 f(k) 操作，直接返回 f(k) 的值。
        // - 3. 若键不存在，则执行 f(k) 得到值 v ，然后将 (k -> v) 存入 hm 中。
        // 该用法用来完成当 f(k) 这个方法需要很多时间执行的时候：
        //   - 若不使用 getOrElseUpdate 方法，则获取 hm(k) 时，每次都需要执行 f(k)。
        //   - 若使用该方法则，在获取 hm(k) 时，只在第一次执行 f(k)，之后都不需要执行 f(k) 直接获取缓存的值。

        val handler = handlers.getOrElseUpdate(signal, {
          logInfo("Registered signal handler for " + signal)
          new ActionHandler(new Signal(signal))
        })
        handler.register(action)
      } catch {
        case ex: Exception => logWarning(s"Failed to register signal handler for " + signal, ex)
      }
    }
  }

  /**
   * A handler for the given signal that runs a collection of actions.
   */
  private class ActionHandler(signal: Signal) extends SignalHandler {

    /**
     * List of actions upon the signal; the callbacks should return true if the signal is "handled",
     * i.e. should not escalate to the next callback.
     *
     * java.util.LinkedList 双向链表：http://tw.gitbook.net/java/util/java_util_linkedlist.html
     * 声明一个双向链表，内容是一个方法，返回值必须是布尔型。
     *
     * actions: java.util.List[() => Boolean] = []
     */
    private val actions = Collections.synchronizedList(new java.util.LinkedList[() => Boolean])

    // original signal handler, before this handler was attached
    private val prevHandler: SignalHandler = Signal.handle(signal, this)

    /**
     * Called when this handler's signal is received. Note that if the same signal is received
     * before this method returns, it is escalated to the previous handler.
     */
    override def handle(sig: Signal): Unit = {
      logInfo("axu.print [core/src/main/scala/org/apache/spark/util/SignalUtils.scala] [Debug] === 这里是 ActionHandler 类 override handle 方法 === sig " + sig.getName())
      // register old handler, will receive incoming signals while this handler is running
      Signal.handle(signal, prevHandler)

      // Run all actions, escalate to parent handler if no action catches the signal
      // (i.e. all actions return false). Note that calling `map` is to ensure that
      // all actions are run, `forall` is short-circuited and will stop evaluating
      // after reaching a first false predicate.
      //
      // 判断 actions 中的元素（函数）返回值是否全部都是 false 。
      // ```
      // scala> actions
      // res20: java.util.List[() => Boolean] = [<function0>, <function0>, <function0>]
      //
      // scala> actions.asScala
      // res21: scala.collection.mutable.Buffer[() => Boolean] = Buffer(<function0>, <function0>, <function0>)
      // 
      // scala> actions.asScala.map(action => action())
      // res22: scala.collection.mutable.Buffer[Boolean] = ArrayBuffer(false, true, false)
      //
      // scala> actions.asScala.map(action => action()).forall(_ == false)
      // res23: Boolean = false
      // ```
      val escalate = actions.asScala.map(action => action()).forall(_ == false)
      logInfo("axu.print [core/src/main/scala/org/apache/spark/util/SignalUtils.scala] [Debug] === 这里是 ActionHandler 类 override handle 方法 === escalate " + escalate)
      if (escalate) {
        prevHandler.handle(sig)
      }

      // re-register this handler
      Signal.handle(signal, this)
    }

    /**
     * Adds an action to be run by this handler.
     * @param action An action to be run when a signal is received. Return true if the signal
     *               should be stopped with this handler, false if it should be escalated.
     *
     * 向链表添加内容，添加一个空函数()，返回值是 action 。
     * java.util.List[() => Boolean] = [<function0>, <function0>, <function0>]
     */
    def register(action: => Boolean): Unit = actions.add(() => action)
  }

  /** Mapping from signal to their respective handlers. */
  private val handlers = new scala.collection.mutable.HashMap[String, ActionHandler]
}
