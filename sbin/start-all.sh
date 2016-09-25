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

# Start all spark daemons.
# Starts the master on this node.
# Starts a worker on each node specified in conf/slaves

echo "axu.print [Log] [1] [sbin/start-all.sh] 执行sbin/start-all.sh"

echo "axu.print [Log] [2] [sbin/start-all.sh] 判断如果SPARK_HOME为空，则设置sbin的父目录为${SPARK_HOME}"
# 如果$SPARK_HOME为空
if [ -z "${SPARK_HOME}" ]; then
  export SPARK_HOME="$(cd "`dirname "$0"`"/..; pwd)"
  echo "axu.print [sbin/start-all.sh] [Define Global] SPARK_HOME: [${SPARK_HOME}]"
fi

echo "axu.print [Log] [3] [sbin/start-all.sh] 调用${SPARK_HOME}/sbin/spark-config.sh"
echo "axu.print [sbin/start-all.sh] <in> [sbin/spark-config.sh]. <=== "
# Load the Spark configuration
. "${SPARK_HOME}/sbin/spark-config.sh"
echo "axu.print [sbin/start-all.sh] <out> [sbin/spark-config.sh]. ===>"

echo "axu.print [Log] [8] [sbin/start-all.sh] 调用${SPARK_HOME}/sbin/start-master.sh"
echo "axu.print [sbin/start-all.sh] <in> [sbin/start-master.sh]. <=== "
# Start Master
"${SPARK_HOME}/sbin"/start-master.sh
echo "axu.print [sbin/start-all.sh] <out> [sbin/start-master.sh]. ===>"

exit 1

# Start Workers
"${SPARK_HOME}/sbin"/start-slaves.sh
