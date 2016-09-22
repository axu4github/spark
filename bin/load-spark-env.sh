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

# This script loads spark-env.sh if it exists, and ensures it is only loaded once.
# spark-env.sh is loaded from SPARK_CONF_DIR if set, or within the current directory's
# conf/ subdirectory.

# Figure out where Spark is installed
echo "axu.print [Log] [19,38,3(bin/spark-class)] [bin/load-spark-env.sh] 判断如果SPARK_HOME为空，则设置sbin的父目录为${SPARK_HOME}"
if [ -z "${SPARK_HOME}" ]; then
  export SPARK_HOME="$(cd "`dirname "$0"`"/..; pwd)"
fi

echo "axu.print [Log] [20,39,4(bin/spark-class)] [bin/load-spark-env.sh] 判断是否已经加载过环境变量，若没有加载过(SPARK_ENV_LOADED为空)，则设置SPARK_ENV_LOADED=1，并判断如果有${SPARK_CONF_DIR}/spark-env.sh文件，讲文件中所有声明的变量全部设置为环境变量"
if [ -z "$SPARK_ENV_LOADED" ]; then
  export SPARK_ENV_LOADED=1
  echo "axu.print [bin/load-spark-env.sh] [Define Global] SPARK_ENV_LOADED: [${SPARK_ENV_LOADED}]"

  # Returns the parent of the directory this script lives in.
  parent_dir="${SPARK_HOME}"

  user_conf_dir="${SPARK_CONF_DIR:-"$parent_dir"/conf}"

  # -f 判断文件是否存在
  if [ -f "${user_conf_dir}/spark-env.sh" ]; then
    # Promote all variable declarations to environment (exported) variables
    set -a
    . "${user_conf_dir}/spark-env.sh"
    set +a
  fi
fi

# Setting SPARK_SCALA_VERSION if not already set.
echo "axu.print [Log] [21,40,5(bin/spark-class)] [bin/load-spark-env.sh] 设置SPARK_SCALA_VERSION"
if [ -z "$SPARK_SCALA_VERSION" ]; then

  ASSEMBLY_DIR2="${SPARK_HOME}/assembly/target/scala-2.11"
  ASSEMBLY_DIR1="${SPARK_HOME}/assembly/target/scala-2.10"

  if [[ -d "$ASSEMBLY_DIR2" && -d "$ASSEMBLY_DIR1" ]]; then
    echo -e "Presence of build for both scala versions(SCALA 2.10 and SCALA 2.11) detected." 1>&2
    echo -e 'Either clean one of them or, export SPARK_SCALA_VERSION=2.11 in spark-env.sh.' 1>&2
    exit 1
  fi

  if [ -d "$ASSEMBLY_DIR2" ]; then
    export SPARK_SCALA_VERSION="2.11"
  else
    export SPARK_SCALA_VERSION="2.10"
  fi
fi
echo "axu.print [bin/load-spark-env.sh] [Define Global] SPARK_SCALA_VERSION: [${SPARK_SCALA_VERSION}]"
