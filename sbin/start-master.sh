#!/usr/bin/env bash

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# Starts the master on the machine this script is executed on.
echo "axu.print [Log] [9] [sbin/start-master.sh] 判断如果SPARK_HOME为空，则设置sbin的父目录为${SPARK_HOME}"
if [ -z "${SPARK_HOME}" ]; then
  export SPARK_HOME="$(cd "`dirname "$0"`"/..; pwd)"
fi

echo "axu.print [Log] [10] [sbin/start-master.sh] 设置变量CLASS为${CLASS}"
# NOTE: This exact class name is matched downstream by SparkSubmit.
# Any changes need to be reflected there.
CLASS="org.apache.spark.deploy.master.Master"

echo "axu.print [Log] [11] [sbin/start-master.sh] 判断传入参数最后是否是--help或者-h，若是则打印Usage"
# $@ 是获取执行命令所有参数
# 例：${SPARK_HOME}/bin/start-master.sh 123 456 789 0 123 
# echo $@ 
# 输出为：123 456 789 0 123
if [[ "$@" = *--help ]] || [[ "$@" = *-h ]]; then
  echo "Usage: ./sbin/start-master.sh [options]"
  pattern="Usage:"
  pattern+="\|Using Spark's default log4j profile:"
  pattern+="\|Registered signal handlers for"

  "${SPARK_HOME}"/bin/spark-class $CLASS --help 2>&1 | grep -v "$pattern" 1>&2
  exit 1
fi

echo "axu.print [Log] [12] [sbin/start-master.sh] 设置变量ORIGINAL_ARGS为$ORIGINAL_ARGS"
# $@ 是获取执行命令所有参数
# 例：${SPARK_HOME}/bin/start-master.sh 123 456 789 0 123 
# echo $@ 
# 输出为：123 456 789 0 123
ORIGINAL_ARGS="$@"

echo "axu.print [Log] [13] [sbin/start-master.sh] 调用${SPARK_HOME}/sbin/spark-config.sh"
echo "axu.print [sbin/start-master.sh] <in> [sbin/spark-config.sh]. <=== "
# - 设置 ${SPARK_HOME} 为 sbin 的父目录
# - 设置 ${SPARK_CONF_DIR} 为 ${SPARK_HOME}/conf
# - 将 ${SPARK_HOME}/python 加入到 PYTHONPATH 中
# - 将 ${SPARK_HOME}/python/lib/py4j-0.10.1-src.zip 加入到 PYTHONPATH 中
. "${SPARK_HOME}/sbin/spark-config.sh"
echo "axu.print [sbin/start-master.sh] <out> [sbin/spark-config.sh]. ===>"

echo "axu.print [Log] [18] [sbin/start-master.sh] 调用${SPARK_HOME}/bin/load-spark-env.sh"
echo "axu.print [sbin/start-master.sh] <in> [bin/load-spark-env.sh]. <=== "
# - 设置 ${SPARK_HOME} 为 sbin 的父目录
# - 通过 conf/spark-env.sh (若文件存在) 设置全局变量
# - 通过编译结果的assembly/target/scala-2.x目录是否存在，设置全局变量${SPARK_SCALA_VERSION}（scala版本）
. "${SPARK_HOME}/bin/load-spark-env.sh"
echo "axu.print [sbin/start-master.sh] <out> [bin/load-spark-env.sh]. ===>"

echo "axu.print [Log] [22] [sbin/start-master.sh] 若SPARK_MASTER_PORT为空，则SPARK_MASTER_PORT为7077"
if [ "$SPARK_MASTER_PORT" = "" ]; then
  SPARK_MASTER_PORT=7077
fi

echo "axu.print [Log] [23] [sbin/start-master.sh] 若SPARK_MASTER_HOST为空，则SPARK_MASTER_HOST为$(hostname)"
if [ "$SPARK_MASTER_HOST" = "" ]; then
  SPARK_MASTER_HOST=`hostname -f`
fi

echo "axu.print [Log] [24] [sbin/start-master.sh] 若SPARK_MASTER_WEBUI_PORT为空，则SPARK_MASTER_WEBUI_PORT为8080"
if [ "$SPARK_MASTER_WEBUI_PORT" = "" ]; then
  SPARK_MASTER_WEBUI_PORT=8080
fi

echo "axu.print [Log] [25] [sbin/start-master.sh] 调用${SPARK_HOME}/sbin/spark-daemon.sh"
echo "axu.print [sbin/start-master.sh] <in> [sbin/spark-daemon.sh]. <=== "
echo "axu.print [sbin/start-master.sh] [Command] [${SPARK_HOME}/sbin/spark-daemon.sh start $CLASS 1 --host $SPARK_MASTER_HOST --port $SPARK_MASTER_PORT --webui-port $SPARK_MASTER_WEBUI_PORT $ORIGINAL_ARGS]"
"${SPARK_HOME}/sbin"/spark-daemon.sh start $CLASS 1 \
  --host $SPARK_MASTER_HOST --port $SPARK_MASTER_PORT --webui-port $SPARK_MASTER_WEBUI_PORT \
  $ORIGINAL_ARGS
echo "axu.print [sbin/start-master.sh] <out> [sbin/spark-daemon.sh]. ===>"
