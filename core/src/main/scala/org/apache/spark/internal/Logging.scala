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

package org.apache.spark.internal

import org.apache.log4j.{Level, LogManager, PropertyConfigurator}
import org.slf4j.{Logger, LoggerFactory}
import org.slf4j.impl.StaticLoggerBinder

import org.apache.spark.util.Utils

/**
 * Utility trait for classes that want to log data. Creates a SLF4J logger for the class and allows
 * logging messages at different levels using methods that only evaluate parameters lazily if the
 * log level is enabled.
 */
private[spark] trait Logging {

  // Make the log field transient so that objects with Logging can
  // be serialized and used on another machine
  @transient private var log_ : Logger = null

  // Method to get the logger name for this object
  // - 14. 获取日志名称
  protected def logName = {
    // Ignore trailing $'s in the class names for Scala objects
    this.getClass.getName.stripSuffix("$")
  }

  // Method to get or create the logger for this object
  protected def log: Logger = {
    // System.err.println(s"axu.print [core/src/main/scala/org/apache/spark/internal/Logging.scala] [Debug] === 这里是 org.apache.spark.internal.Logging.log 方法 ===")
    // 由于基本所有程序都会调用log方法，若以日志的方式打印内容会打出很多日志。
    // 所以这里使用注释的行驶说明程序运行流程以及调用方式
    // - 1. 单例模式，创建or返回log_对象
    if (log_ == null) {
      // - 2. 调用initializeLogIfNecessary，返回log_
      initializeLogIfNecessary(false)
      // - 13. 获取日志对象
      log_ = LoggerFactory.getLogger(logName)

      System.err.println(s"axu.print [core/src/main/scala/org/apache/spark/internal/Logging.scala] [Debug] logName: [$logName]")
    }
    log_
  }

  // Log methods that take only a String
  protected def logInfo(msg: => String) {
    if (log.isInfoEnabled) log.info(msg)
  }

  protected def logDebug(msg: => String) {
    if (log.isDebugEnabled) log.debug(msg)
  }

  protected def logTrace(msg: => String) {
    if (log.isTraceEnabled) log.trace(msg)
  }

  protected def logWarning(msg: => String) {
    if (log.isWarnEnabled) log.warn(msg)
  }

  protected def logError(msg: => String) {
    if (log.isErrorEnabled) log.error(msg)
  }

  // Log methods that take Throwables (Exceptions/Errors) too
  protected def logInfo(msg: => String, throwable: Throwable) {
    if (log.isInfoEnabled) log.info(msg, throwable)
  }

  protected def logDebug(msg: => String, throwable: Throwable) {
    if (log.isDebugEnabled) log.debug(msg, throwable)
  }

  protected def logTrace(msg: => String, throwable: Throwable) {
    if (log.isTraceEnabled) log.trace(msg, throwable)
  }

  protected def logWarning(msg: => String, throwable: Throwable) {
    if (log.isWarnEnabled) log.warn(msg, throwable)
  }

  protected def logError(msg: => String, throwable: Throwable) {
    if (log.isErrorEnabled) log.error(msg, throwable)
  }

  protected def isTraceEnabled(): Boolean = {
    log.isTraceEnabled
  }

  // - 3. 初始化
  protected def initializeLogIfNecessary(isInterpreter: Boolean): Unit = {
    // - 4. 调用 ojbect Logging initialized 变量
    if (!Logging.initialized) {
      // - 8. 基于多线程的同步锁处理，保证在多线程的环境中不会同时访问
      Logging.initLock.synchronized {
        if (!Logging.initialized) {
          // - 9. 调用initializeLogging方法
          initializeLogging(isInterpreter)
        }
      }
    }
  }

  private def initializeLogging(isInterpreter: Boolean): Unit = {
    // Don't use a logger in here, as this is itself occurring during initialization of a logger
    // If Log4j 1.2 is being used, but is not initialized, load a default properties file
    val binderClass = StaticLoggerBinder.getSingleton.getLoggerFactoryClassStr
    // This distinguishes the log4j 1.2 binding, currently
    // org.slf4j.impl.Log4jLoggerFactory, from the log4j 2.0 binding, currently
    // org.apache.logging.slf4j.Log4jLoggerFactory
    // - 10. 判断是否是使用log4j 1.2
    val usingLog4j12 = "org.slf4j.impl.Log4jLoggerFactory".equals(binderClass)
    if (usingLog4j12) {
      val log4j12Initialized = LogManager.getRootLogger.getAllAppenders.hasMoreElements
      // scalastyle:off println
      if (!log4j12Initialized) {
        val defaultLogProps = "org/apache/spark/log4j-defaults.properties"
        // - 11. 调用'org.apache.spark.util.Utils.getSparkClassLoader'，获取defaultLogProps对应的日志配置url
        Option(Utils.getSparkClassLoader.getResource(defaultLogProps)) match {
          case Some(url) =>
            // - 12. 执行日志配置
            // url ==> jar:file:${SPARK_HOME}/assembly/target/scala-2.11/jars/spark-core_2.11-2.1.0-SNAPSHOT.jar!/org/apache/spark/log4j-defaults.properties
            // 真正配置文件位置为 ${SPARK_HOME}/core/src/main/resources/org/apache/spark/log4j-defaults.properties
            PropertyConfigurator.configure(url)
            System.err.println(s"Using Spark's default log4j profile: $defaultLogProps")
            System.err.println(s"axu.print [core/src/main/scala/org/apache/spark/internal/Logging.scala] [Debug] log4j configure url: [$url]")
          case None =>
            System.err.println(s"Spark was unable to load $defaultLogProps")
        }
      }

      if (isInterpreter) {
        // Use the repl's main class to define the default log level when running the shell,
        // overriding the root logger's config if they're different.
        val rootLogger = LogManager.getRootLogger()
        val replLogger = LogManager.getLogger(logName)
        val replLevel = Option(replLogger.getLevel()).getOrElse(Level.WARN)
        if (replLevel != rootLogger.getEffectiveLevel()) {
          System.err.printf("Setting default log level to \"%s\".\n", replLevel)
          System.err.println("To adjust logging level use sc.setLogLevel(newLevel). " +
            "For SparkR, use setLogLevel(newLevel).")
          rootLogger.setLevel(replLevel)
        }
      }
      // scalastyle:on println
    }
    Logging.initialized = true

    // Force a call into slf4j to initialize it. Avoids this happening from multiple threads
    // and triggering this: http://mailman.qos.ch/pipermail/slf4j-dev/2010-April/002956.html
    log
  }
}

// - 5. 通过静态方法 调用 object 对象
private object Logging {

  // - 6. 声明变量
  // #!# @volatile 是什么意思不知
  @volatile private var initialized = false
  val initLock = new Object()
  try {
    // We use reflection here to handle the case where users remove the
    // slf4j-to-jul bridge order to route their logs to JUL.
    // - 7. 基于'org.slf4j.bridge.SLF4JBridgeHandler'的日志重定向功能初始化
    val bridgeClass = Utils.classForName("org.slf4j.bridge.SLF4JBridgeHandler")
    bridgeClass.getMethod("removeHandlersForRootLogger").invoke(null)
    val installed = bridgeClass.getMethod("isInstalled").invoke(null).asInstanceOf[Boolean]
    if (!installed) {
      bridgeClass.getMethod("install").invoke(null)
    }
  } catch {
    case e: ClassNotFoundException => // can't log anything yet so just fail silently
  }
}
