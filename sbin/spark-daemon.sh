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

# Runs a Spark command as a daemon.
#
# Environment Variables
#
#   SPARK_CONF_DIR  Alternate conf dir. Default is ${SPARK_HOME}/conf.
#   SPARK_LOG_DIR   Where log files are stored. ${SPARK_HOME}/logs by default.
#   SPARK_MASTER    host:path where spark code should be rsync'd from
#   SPARK_PID_DIR   The pid files are stored. /tmp by default.
#   SPARK_IDENT_STRING   A string representing this instance of spark. $USER by default
#   SPARK_NICENESS The scheduling priority for daemons. Defaults to 0.
##

usage="Usage: spark-daemon.sh [--config <conf-dir>] (start|stop|submit|status) <spark-command> <spark-instance-number> <args...>"

echo "axu.print [Log] [26] [sbin/start-daemon.sh] 如果传入参数小于1个则退出并打印usage"
# - $# 是打印传入参数个数
# - le 是判断是否小于
# - 判断如果没有传入参数，则输出usage
# if no args specified, show usage
if [ $# -le 1 ]; then
  echo $usage
  exit 1
fi

echo "axu.print [Log] [27] [sbin/start-daemon.sh] 判断如果SPARK_HOME为空，则设置sbin的父目录为${SPARK_HOME}"
if [ -z "${SPARK_HOME}" ]; then
  export SPARK_HOME="$(cd "`dirname "$0"`"/..; pwd)"
fi

echo "axu.print [Log] [28] [sbin/start-daemon.sh] 调用${SPARK_HOME}/sbin/spark-config.sh"
echo "axu.print [sbin/start-daemon.sh] <in> [sbin/spark-config.sh]. <=== "
# - 设置 ${SPARK_HOME} 为 sbin 的父目录
# - 设置 ${SPARK_CONF_DIR} 为 ${SPARK_HOME}/conf
# - 将 ${SPARK_HOME}/python 加入到 PYTHONPATH 中
# - 将 ${SPARK_HOME}/python/lib/py4j-0.10.1-src.zip 加入到 PYTHONPATH 中
. "${SPARK_HOME}/sbin/spark-config.sh"
echo "axu.print [sbin/start-daemon.sh] <out> [sbin/spark-config.sh]. ===>"

# get arguments

# Check if --config is passed as an argument. It is an optional parameter.
# Exit if the argument is not a directory.

echo "axu.print [Log] [33] [sbin/start-daemon.sh] 若传入第一个参数为'--config'，设置conf_dir为'--config'下一个参数（--config的值），若conf_dir不存在则报错退出，若存在则设置全局变量SPARK_CONF_DIR为conf_dir"
# - 判断第一个输入参数是否为"--config"
# - shift 命令是参数左移命令，若只有shift的话，可以理解为next(下一个参数)
# - 声明conf_dir为--config的下个参数值
#   比如输入命令若为 ${SPARK_HOME}/sbin/spark-daemon.sh --config ${SPARK_HOME}/conf/ 
#   则 $1 首先是 --config，然后执行shift后，整体参数左移（原 $1 丢失，$1 = $2），那么 $1 就应该是 ${SPARK_HOME}/conf/
# - 判断${conf_dir}目录是否存在  
# - 若不存在，则退出脚本，输出错误信息和使用说明（usage）
# - 若存在，则设置全局变量${SPARK_CONF_DIR}
if [ "$1" == "--config" ]
then
  shift
  conf_dir="$1"
  if [ ! -d "$conf_dir" ]
  then
    echo "ERROR : $conf_dir is not a directory"
    echo $usage
    exit 1
  else
    export SPARK_CONF_DIR="$conf_dir"
  fi
  shift
fi
echo "axu.print [sbin/start-daemon.sh] [Define Global] SPARK_CONF_DIR: [${SPARK_CONF_DIR}]"

echo "axu.print [Log] [34] [sbin/start-daemon.sh] 设置option为下一个参数"
# - 操作命令 可选项(start|stop|submit|status)
# - 在sbin/start-master.sh调用过程中 option="start"（具体详见${SPARK_HOME}/sbin/start-master.sh）
option=$1
# - 参数左移1位（指定option下一个参数为$1）
shift
echo "axu.print [sbin/start-daemon.sh] [Debug] option: [${option}]"

echo "axu.print [Log] [35] [sbin/start-daemon.sh] 设置command为下一个参数"
# - spark-command
# - 在sbin/start-master.sh调用过程中 command="$CLASS"（org.apache.spark.deploy.master.Master）（具体详见${SPARK_HOME}/sbin/start-master.sh）
command=$1
# - 参数左移1位（指定command下一个参数为$1）
shift
echo "axu.print [sbin/start-daemon.sh] [Debug] command: [${command}]"

echo "axu.print [Log] [36] [sbin/start-daemon.sh] 设置instance为下一个参数"
# - spark-instance-number
# - 在sbin/start-master.sh调用过程中 instance=1（具体详见${SPARK_HOME}/sbin/start-master.sh）
instance=$1
# - 参数左移1位（指定instance下一个参数为$1）
shift
echo "axu.print [sbin/start-daemon.sh] [Debug] instance: [${instance}]"

# - 声明spark_rotate_log()方法
# - 参数1：日志文件（全路径）
# - 参数2：次数 [可选]，默认为5
# - 轮训日志文件，将最后一个文件（${log}.${num}）文件删除
spark_rotate_log ()
{
  echo "axu.print [Log] [54] [sbin/start-daemon.sh] [function:spark_rotate_log] 设置log为第一个参数" 
  log=$1;
  # echo "axu.print [sbin/start-daemon.sh] [Debug] log: [${log}]"
  num=5;

  echo "axu.print [Log] [55] [sbin/start-daemon.sh] [function:spark_rotate_log] 若传入第二个参数，则设置num为第二个参数" 
  # - 若传入第二个参数，则${num}=$2
  if [ -n "$2" ]; then
    num=$2
  fi
  echo "axu.print [sbin/start-daemon.sh] [Debug] num: [${num}]"

  echo "axu.print [Log] [55] [sbin/start-daemon.sh] [function:spark_rotate_log] 循环num次，删除最后一个若有num个日志文件，若没有num个日志文件则向下创建一个，最大有num+1个日志文件（log + log.{num}）"
  # - 若日志文件存在
  # - 循环${num}次
  # - 轮训日志文件，将最后一个文件（${log}.${num}）文件删除
  if [ -f "$log" ]; then # rotate logs
    while [ $num -gt 1 ]; do
      prev=`expr $num - 1`
      [ -f "$log.$prev" ] && mv "$log.$prev" "$log.$num"
      num=$prev
    done
    mv "$log" "$log.$num";
  fi
}

echo "axu.print [Log] [37] [sbin/start-daemon.sh] 调用${SPARK_HOME}/bin/load-spark-env.sh"
echo "axu.print [sbin/start-daemon.sh] <in> [bin/load-spark-env.sh]. <=== "
# - 设置 ${SPARK_HOME} 为 sbin 的父目录
# - 通过 conf/spark-env.sh (若文件存在) 设置全局变量
# - 通过编译结果的assembly/target/scala-2.x目录是否存在，设置全局变量${SPARK_SCALA_VERSION}（scala版本）
. "${SPARK_HOME}/bin/load-spark-env.sh"
echo "axu.print [sbin/start-daemon.sh] <out> [bin/load-spark-env.sh]. ===>"


echo "axu.print [Log] [41] [sbin/start-daemon.sh] 如果没有设置全局变量SPARK_IDENT_STRING，则设置为当前用户"
# - 设置启动用户，默认为${USER}，系统当前用户
if [ "$SPARK_IDENT_STRING" = "" ]; then
  export SPARK_IDENT_STRING="$USER"
fi
echo "axu.print [sbin/start-daemon.sh] [Define Global] SPARK_IDENT_STRING: [${SPARK_IDENT_STRING}]"

echo "axu.print [Log] [42] [sbin/start-daemon.sh] 设置全局变量SPARK_PRINT_LAUNCH_COMMAND为1"
# - 设置全部变量${SPARK_PRINT_LAUNCH_COMMAND}为1 #!# ${SPARK_PRINT_LAUNCH_COMMAND} 具体作用未知
export SPARK_PRINT_LAUNCH_COMMAND="1"
echo "axu.print [sbin/start-daemon.sh] [Define Global] SPARK_PRINT_LAUNCH_COMMAND: [${SPARK_PRINT_LAUNCH_COMMAND}]"

echo "axu.print [Log] [43] [sbin/start-daemon.sh] 设置全局变量SPARK_LOG_DIR，则设置为${SPARK_HOME}/logs，并创建，同时向SPARK_LOG_DIR目录中创建一个测试文件.spark_test，若执行成功则删除测试文件，若失败则将SPARK_LOG_DIR目录用户设置为SPARK_IDENT_STRING"
# get log directory
# - 如果没有设置日志目录，则设置全局变量${SPARK_LOG_DIR}="${SPARK_HOME}/logs"
if [ "$SPARK_LOG_DIR" = "" ]; then
  export SPARK_LOG_DIR="${SPARK_HOME}/logs"
fi
echo "axu.print [sbin/start-daemon.sh] [Define Global] SPARK_LOG_DIR: [${SPARK_LOG_DIR}]"

# - 创建日志目录
mkdir -p "$SPARK_LOG_DIR"

# - 在日志目录中创建测试文件
touch "$SPARK_LOG_DIR"/.spark_test > /dev/null 2>&1

# - $?是最后运行的命令的返回值，若为0则证明命令执行成功
# - TEST_LOG_DIR=$?的意思是获取上条"在日志目录中创建测试文件"的命令执行返回值
# - 判断如果执行成功（返回值为0）则删除创建文件，若不成功则赋权
TEST_LOG_DIR=$?
if [ "${TEST_LOG_DIR}" = "0" ]; then
  rm -f "$SPARK_LOG_DIR"/.spark_test
else
  chown "$SPARK_IDENT_STRING" "$SPARK_LOG_DIR"
fi
echo "axu.print [sbin/start-daemon.sh] [Debug] SPARK_LOG_DIR: [${SPARK_LOG_DIR}]"

echo "axu.print [Log] [44] [sbin/start-daemon.sh] 若没有设置SPARK_PID_DIR目录，则设置为/tmp"
# - 如果${SPARK_PID_DIR}没有设置，则设置为/tmp
if [ "$SPARK_PID_DIR" = "" ]; then
  SPARK_PID_DIR=/tmp
fi
echo "axu.print [sbin/start-daemon.sh] [Debug] SPARK_PID_DIR: [${SPARK_PID_DIR}]"

echo "axu.print [Log] [45] [sbin/start-daemon.sh] 设置日志文件和pid文件名称"
# some variables
log="$SPARK_LOG_DIR/spark-$SPARK_IDENT_STRING-$command-$instance-$HOSTNAME.out"
pid="$SPARK_PID_DIR/spark-$SPARK_IDENT_STRING-$command-$instance.pid"
echo "axu.print [sbin/start-daemon.sh] [Debug] log: [${log}]"
echo "axu.print [sbin/start-daemon.sh] [Debug] pid: [${pid}]"

echo "axu.print [Log] [46] [sbin/start-daemon.sh] 设置运行优先级SPARK_NICENESS，若没设置SPARK_NICENESS，则为0"
# Set default scheduling priority
# 如果没有设置${SPARK_NICENESS}则设置全局变量${SPARK_NICENESS}=0（看注释应该是调度优先级的意思）
if [ "$SPARK_NICENESS" = "" ]; then
    export SPARK_NICENESS=0
fi
echo "axu.print [sbin/start-daemon.sh] [Define Global] SPARK_NICENESS: [${SPARK_NICENESS}]"

# - 定义执行命令方法
run_command() {
  echo "axu.print [Log] [49] [sbin/start-daemon.sh] [function:run_command] 设置mode为第一个参数" 
  # - 声明变量${mode}
  # - 在sbin/start-master.sh 中 ${mode}="class"
  # - shift 参数左移一位
  mode="$1"
  shift
  echo "axu.print [sbin/start-daemon.sh] [Debug] mode: [${mode}]"

  echo "axu.print [Log] [50] [sbin/start-daemon.sh] [function:run_command] 创建SPARK_PID_DIR目录（默认为/tmp）" 
  # - 创建存放pid目录${SPARK_PID_DIR}（若没有设置默认为/tmp）
  mkdir -p "$SPARK_PID_DIR"

  echo "axu.print [Log] [51] [sbin/start-daemon.sh] [function:run_command] 如果pid文件存在，读取pid文件内容得到TARGET_ID，根据TARGET_ID找到进程启动命令，并判断是否是java进程，若是说明已启动并退出" 
  # - 如果${pid}文件存在
  # - 将${pid}文件内容，赋值给${TARGET_ID}
  # - ps参数说明：
  #   - [-p] 进程号
  #   - [-o] 查看哪列
  #   - 最后的"="号是去除列名
  # - 根据${TARGET_ID}（进程号）通过ps命令找到该进程号执行命令，并判断执行命令中是否包含"java"
  # - 如果执行命令包含"java"则证明${command}正在运行，之后退出
  # - #!# if中的"=~"" 具体含义不清楚
  if [ -f "$pid" ]; then
    TARGET_ID="$(cat "$pid")"
    echo "axu.print [sbin/start-daemon.sh] [Debug] TARGET_ID: [${TARGET_ID}]"
    echo "axu.print [sbin/start-daemon.sh] [Debug] TARGET_ID_PROCESS: [$(ps -p "$TARGET_ID" -o comm=)]"
    if [[ $(ps -p "$TARGET_ID" -o comm=) =~ "java" ]]; then
      echo "$command running as process $TARGET_ID.  Stop it first."
      exit 1
    fi
  fi

  echo "axu.print [Log] [52] [sbin/start-daemon.sh] [function:run_command] 若全局变量SPARK_MASTER不为空，则需要同步信息 #!# 具体不知"
  # - #!# 不懂，不知道${SPARK_MASTER}不为空的时候是什么内容
  # - 当${SPARK_MASTER}不为空时，和${SPARK_MASTER}做同步 #!#
  if [ "$SPARK_MASTER" != "" ]; then
    echo rsync from "$SPARK_MASTER"
    rsync -a -e ssh --delete --exclude=.svn --exclude='logs/*' --exclude='contrib/hod/logs/*' "$SPARK_MASTER/" "${SPARK_HOME}"
  fi
  
  echo "axu.print [Log] [53] [sbin/start-daemon.sh] [function:run_command] 调用 spark_rotate_log 方法"
  echo "axu.print [sbin/start-daemon.sh] [function:run_command] <in function> [spark_rotate_log] ['$log']. <=== "
  # - 调用spark_rotate_log()方法，$1=${log}文件（全路径）
  # - 若${log}日志文件存在，则轮训日志文件，将最后一个文件（${log}.${num}）文件删除
  spark_rotate_log "$log"
  echo "axu.print [sbin/start-daemon.sh] [function:run_command] <out function> [spark_rotate_log] []. ===>"

  echo "starting $command, logging to $log"

  echo "axu.print [Log] [56] [sbin/start-daemon.sh] [function:run_command] 根据 mode 执行不同命令"
  # - 根据${mode}调用文件
  #   - 若 sbin/start-master.sh 调用，则执行 (class) 实际执行为：
  #     - nohup nice -n 0 "${SPARK_HOME}"/bin/spark-class "org.apache.spark.deploy.master.Master" \ 
  #                                                       --host $SPARK_MASTER_HOST \ 
  #                                                       --port $SPARK_MASTER_PORT \
  #                                                       --webui-port $SPARK_MASTER_WEBUI_PORT \
  #                                                       $ORIGINAL_ARGS
  # - 获取 启动/提交 命令进程的进程号（pid），将它赋给${newpid}变量
  # - nice -n 是设置系统进程优先级，-n 值越大优先级越低
  case "$mode" in
    (class)
      echo "axu.print [Log] [57] [sbin/start-daemon.sh] [function:run_command] 异步(nohup)调用 bin/spark-class，并获取启动进程pid，设置成newpid"
      echo "axu.print [sbin/start-daemon.sh] <in> [bin/spark-class]. <=== "
      echo "axu.print [sbin/start-daemon.sh] [Command] [nohup nice -n $SPARK_NICENESS ${SPARK_HOME}/bin/spark-class $command $@ >> $log 2>&1 < /dev/null &]"
      nohup nice -n "$SPARK_NICENESS" "${SPARK_HOME}"/bin/spark-class $command "$@" >> "$log" 2>&1 < /dev/null &
      newpid="$!"
      echo "axu.print [sbin/start-daemon.sh] <out> [bin/spark-class]. ===> "
      echo "axu.print [sbin/start-daemon.sh] [Debug] newpid: [${newpid}]"
      ;;

    (submit)
      nohup nice -n "$SPARK_NICENESS" "${SPARK_HOME}"/bin/spark-submit --class $command "$@" >> "$log" 2>&1 < /dev/null &
      newpid="$!"
      ;;

    (*)
      echo "unknown mode: $mode"
      exit 1
      ;;
  esac

  # - 将 启动/提交 进程的进程号（pid），写入${pid}文件中
  echo "$newpid" > "$pid"
  
  #Poll for up to 5 seconds for the java process to start
  # - 循环10次，每次歇0.5秒，查看 启动/提交 进程是否启动
  for i in {1..10}
  do
    if [[ $(ps -p "$newpid" -o comm=) =~ "java" ]]; then
       break
    fi
    sleep 0.5
  done

  sleep 2

  # Check if the process has died; in that case we'll tail the log so the user can see
  # - 休息2秒后，查看刚 启动/提交 进程是否关闭（出错），若关闭则报错，并打印出最后两行日志，并输出日志文件位置
  if [[ ! $(ps -p "$newpid" -o comm=) =~ "java" ]]; then
    echo "failed to launch $command:"
    tail -2 "$log" | sed 's/^/  /'
    echo "full log in $log"
  fi
}

echo "axu.print [Log] [47] [sbin/start-daemon.sh] 根据option执行不同命令"
# - 判断 ${option} 变量
# - 在sbin/start-master.sh 时 ${option}="start"，
#   - 则应执行 run_command class "$@" （$@是返回当前所有参数的意思（因为之前代码执行过shift，部分参数已经被丢弃））
#   - 则在sbin/start-master.sh调用时，"$@"应为"--host $SPARK_MASTER_HOST --port $SPARK_MASTER_PORT --webui-port $SPARK_MASTER_WEBUI_PORT $ORIGINAL_ARGS"部分（之前的参数已经被shift丢弃）
case $option in

  (submit)
    run_command submit "$@"
    ;;

  (start)
    echo "axu.print [Log] [48] [sbin/start-daemon.sh] 调用 run_command 方法"
    echo "axu.print [sbin/start-daemon.sh] <in function> [run_command] [class '$@']. <=== "
    run_command class "$@"
    echo "axu.print [sbin/start-daemon.sh] <out function> [run_command] []. ===>"
    ;;

  (stop)

    if [ -f $pid ]; then
      TARGET_ID="$(cat "$pid")"
      if [[ $(ps -p "$TARGET_ID" -o comm=) =~ "java" ]]; then
        echo "stopping $command"
        kill "$TARGET_ID" && rm -f "$pid"
      else
        echo "no $command to stop"
      fi
    else
      echo "no $command to stop"
    fi
    ;;

  (status)

    if [ -f $pid ]; then
      TARGET_ID="$(cat "$pid")"
      if [[ $(ps -p "$TARGET_ID" -o comm=) =~ "java" ]]; then
        echo $command is running.
        exit 0
      else
        echo $pid file is present but $command not running
        exit 1
      fi
    else
      echo $command not running.
      exit 2
    fi
    ;;

  (*)
    echo $usage
    exit 1
    ;;

esac
