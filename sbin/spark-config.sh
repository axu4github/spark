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

# included in all the spark scripts with source command
# should not be executable directly
# also should not be passed any arguments, since we need original $*
echo "axu.print [Log] [4,14,29] [sbin/spark-config.sh] 判断如果SPARK_HOME为空，则设置sbin的父目录为${SPARK_HOME}"
# symlink and absolute path should rely on SPARK_HOME to resolve
if [ -z "${SPARK_HOME}" ]; then
  export SPARK_HOME="$(cd "`dirname "$0"`"/..; pwd)"
  echo "axu.print [sbin/spark-config.sh] [Define Global] SPARK_HOME: [${SPARK_HOME}]"
fi

echo "axu.print [Log] [5,15,30] [sbin/spark-config.sh] 判断如果${SPARK_CONF_DIR}为空，则定义${SPARK_CONF_DIR}为${SPARK_HOME}/conf"
# 如果$SPARK_CONF_DIR不存在或者为空，那么${SPARK_HOME}/conf等于$SPARK_CONF_DIR
# 定义${SPARK_CONF_DIR}
export SPARK_CONF_DIR="${SPARK_CONF_DIR:-"${SPARK_HOME}/conf"}"
echo "axu.print [sbin/spark-config.sh] [Define Global] SPARK_CONF_DIR: [${SPARK_CONF_DIR}]"


# Add the PySpark classes to the PYTHONPATH:
if [ -z "${PYSPARK_PYTHONPATH_SET}" ]; then
  echo "axu.print [Log] [6,16,31] [sbin/spark-config.sh] 定义PYTHONPATH"
  export PYTHONPATH="${SPARK_HOME}/python:${PYTHONPATH}"

  echo "axu.print [Log] [7,17,32] [sbin/spark-config.sh] 再次定义PYTHONPATH"
  export PYTHONPATH="${SPARK_HOME}/python/lib/py4j-0.10.3-src.zip:${PYTHONPATH}"
  export PYSPARK_PYTHONPATH_SET=1
fi 

echo "axu.print [sbin/spark-config.sh] [Define Global] PYTHONPATH: [${PYTHONPATH}]"
