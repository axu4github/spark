#!/bin/sh

# --- 编译spark --- 
# 参考：http://spark.apache.org/docs/latest/building-spark.html
# 
# 只编译单个模块命令：./build/mvn -pl :spark-streaming_2.11 clean install
# 其中spark-streaming_2.11的名称是在模块对应的pom.xml文件中（streaming/pom.xml）。
# 
# - org.apache.spark.launcher 模块名称：spark-launcher_2.11
#

usage="Usage: build_command.sh (all|test|launcher)"

if [ $# -lt 1 ]; then
  echo $usage
  exit 1
fi

# 设置SPARK_HOME
if [ -z "${SPARK_HOME}"]; then
  export SPARK_HOME="$(cd "$(dirname $0)"; pwd)"
fi

# 切换到SPARK_HOME
cd ${SPARK_HOME}

# 切换scala版本
# 编译2.0.0时，scala使用2.10版本会有错误，所以推荐使用2.11版本
# ./dev/change-scala-version.sh 2.10

# 设置maven可使用内存，若不设置可能会导致如下错误
# ```
# [INFO] Compiling 203 Scala sources and 9 Java sources to /Users/me/Development/spark/core/target/scala-2.11/classes...
# [ERROR] PermGen space -> [Help 1]
# 
# [INFO] Compiling 203 Scala sources and 9 Java sources to /Users/me/Development/spark/core/target/scala-2.11/classes...
# [ERROR] Java heap space -> [Help 1]
# ```
export MAVEN_OPTS="-Xmx2g -XX:MaxPermSize=512M -XX:ReservedCodeCacheSize=512m"



case $1 in
    # 如果是全部编译，则执行编译后退出
    "all") 
        # 执行编译（使用内置的maven）
        # 使用内置的maven编译会自动下载所需要的包（Maven, Scala和Zinc）
        # -Pyarn 支持yarn
        # -Phadoop-2.7 -Dhadoop.version=VERSION 支持hadoop 2.7+
        # -Phive -Phive-thriftserver 支持hive和JDBC
        # -Dscala-2.10 使用scala 2.10（spark生产环境使用的是2.10）
        ./build/mvn -Pyarn -Phadoop-2.4 -Dhadoop.version=2.4.0 -Phive -Phive-thriftserver -DskipTests clean package
        exit 1
    ;;
    "test")
        ./build/mvn -Pyarn -Phadoop-2.4 -DskipTests -Phive -Phive-thriftserver clean package
        exit 1
    ;;
    "launcher") 
        submodule="spark-launcher_2.11"
    ;;
    "core")
        submodule="spark-core_2.11"
    ;;
    *) 
        echo $usage
        exit 1
    ;;
esac

# 编译单个模块
CMD="./build/mvn -pl :${submodule} clean install"
echo "${CMD}"
${CMD}

 
# 返回执行命令目录
# cd -
# -- EOF ---
