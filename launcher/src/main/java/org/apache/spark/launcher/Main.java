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

package org.apache.spark.launcher;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.spark.launcher.CommandBuilderUtils.*;

/**
 * Command line interface for the Spark launcher. Used internally by Spark scripts.
 */
class Main {

  /**
   * Usage: Main [class] [class args]
   * <p>
   * This CLI works in two different modes:
   * <ul>
   *   <li>"spark-submit": if <i>class</i> is "org.apache.spark.deploy.SparkSubmit", the
   *   {@link SparkLauncher} class is used to launch a Spark application.</li>
   *   <li>"spark-class": if another class is provided, an internal Spark class is run.</li>
   * </ul>
   *
   * This class works in tandem with the "bin/spark-class" script on Unix-like systems, and
   * "bin/spark-class2.cmd" batch script on Windows to execute the final command.
   * <p>
   * On Unix-like systems, the output is a list of command arguments, separated by the NULL
   * character. On Windows, the output is a command line suitable for direct execution from the
   * script.
   */
  public static void main(String[] argsArray) throws Exception {
    // CommandBuilderUtils.checkArgument() 判断获取参数数量，若没有参数则报提示错误
    checkArgument(argsArray.length > 0, "Not enough arguments: missing class name.");

    List<String> args = new ArrayList<>(Arrays.asList(argsArray));

    // 获取第一个参数
    String className = args.remove(0);

    // CommandBuilderUtils.isEmpty() 判断是否设置全局变量 SPARK_PRINT_LAUNCH_COMMAND
    boolean printLaunchCommand = !isEmpty(System.getenv("SPARK_PRINT_LAUNCH_COMMAND"));
    AbstractCommandBuilder builder;

    // 如果是提交操作
    if (className.equals("org.apache.spark.deploy.SparkSubmit")) {
      try {
        builder = new SparkSubmitCommandBuilder(args);
      } catch (IllegalArgumentException e) {
        printLaunchCommand = false;
        System.err.println("Error: " + e.getMessage());
        System.err.println();

        MainClassOptionParser parser = new MainClassOptionParser();
        try {
          parser.parse(args);
        } catch (Exception ignored) {
          // Ignore parsing exceptions.
        }

        List<String> help = new ArrayList<>();
        if (parser.className != null) {
          help.add(parser.CLASS);
          help.add(parser.className);
        }
        help.add(parser.USAGE_ERROR);
        builder = new SparkSubmitCommandBuilder(help);
      }

    // 如果是其他（启动/停止/...）操作
    } else {
        // SparkClassCommandBuilder 类也继承 AbstractCommandBuilder
        // 调用SparkClassCommandBuilder()
        builder = new SparkClassCommandBuilder(className, args);
    }

    Map<String, String> env = new HashMap<>();

    // 调用SparkClassCommandBuilder.buildCommand()
    List<String> cmd = builder.buildCommand(env);
    if (printLaunchCommand) {
      // Spark Command: 
      // /Library/Java/JavaVirtualMachines/jdk1.7.0_79.jdk/Contents/Home/bin/java 
      //  -cp 
      //  /Users/axu/code/axuProject/spark-2.0.0-hadoop2.4/conf/:/Users/axu/code/axuProject/spark-2.0.0-hadoop2.4/assembly/target/scala-2.11/jars/* 
      //  -Xmx1g 
      //  -XX:MaxPermSize=256m 
      //  org.apache.spark.deploy.master.Master 
      //  --host axu4iMac.local --port 7077 --webui-port 8080
      System.err.println(" --- axu.print ---");
      System.err.println("Spark Command: " + join(" ", cmd));
      System.err.println("========================================");
    }

    if (isWindows()) {
      System.out.println(prepareWindowsCommand(cmd, env));
    } else {
      // In bash, use NULL as the arg separator since it cannot be used in an argument.
      // cmd(List)组成：
      //   - 调用 buildJavaCommand()
      //     - ${JAVA_HOME}/bin/java
      //     - 若 ${SPARK_HOME}/conf/java-opts 存在，将文件设置的内容加入
      //     - "-cp"
      //     - 调用 buildClassPath() 
      //       - ${SPARK_CLASSPATH}:${SPARK_CONF_DIR}:${jarDirs}:${HADOOP_CONF_DIR}:${YARN_CONF_DIR}:${SPARK_DIST_CLASSPATH}
      //   - ${javaOptsKeys(List)}
      //   - "-Xmx" + ${memKey变量值}|DEFAULT_MEM 
      //   - 调用 addPermGenSizeOpt()
      //     - "-XX:MaxPermSize=256m"
      //   - className变量值
      //   - classArgs变量值
      List<String> bashCmd = prepareBashCommand(cmd, env);

      // 依次将cmd(List)输出到终端上，使用'\0'分割
      for (String c : bashCmd) {
        System.out.print(c);
        System.out.print('\0');
      }
    }
  }

  /**
   * Prepare a command line for execution from a Windows batch script.
   *
   * The method quotes all arguments so that spaces are handled as expected. Quotes within arguments
   * are "double quoted" (which is batch for escaping a quote). This page has more details about
   * quoting and other batch script fun stuff: http://ss64.com/nt/syntax-esc.html
   */
  private static String prepareWindowsCommand(List<String> cmd, Map<String, String> childEnv) {
    StringBuilder cmdline = new StringBuilder();
    for (Map.Entry<String, String> e : childEnv.entrySet()) {
      cmdline.append(String.format("set %s=%s", e.getKey(), e.getValue()));
      cmdline.append(" && ");
    }
    for (String arg : cmd) {
      cmdline.append(quoteForBatchScript(arg));
      cmdline.append(" ");
    }
    return cmdline.toString();
  }

  /**
   * Prepare the command for execution from a bash script. The final command will have commands to
   * set up any needed environment variables needed by the child process.
   */
  private static List<String> prepareBashCommand(List<String> cmd, Map<String, String> childEnv) {
    if (childEnv.isEmpty()) {
      return cmd;
    }

    List<String> newCmd = new ArrayList<>();
    newCmd.add("env");

    for (Map.Entry<String, String> e : childEnv.entrySet()) {
      newCmd.add(String.format("%s=%s", e.getKey(), e.getValue()));
    }
    newCmd.addAll(cmd);
    return newCmd;
  }

  /**
   * A parser used when command line parsing fails for spark-submit. It's used as a best-effort
   * at trying to identify the class the user wanted to invoke, since that may require special
   * usage strings (handled by SparkSubmitArguments).
   */
  private static class MainClassOptionParser extends SparkSubmitOptionParser {

    String className;

    @Override
    protected boolean handle(String opt, String value) {
      if (CLASS.equals(opt)) {
        className = value;
      }
      return false;
    }

    @Override
    protected boolean handleUnknown(String opt) {
      return false;
    }

    @Override
    protected void handleExtraArgs(List<String> extra) {

    }

  }

}
