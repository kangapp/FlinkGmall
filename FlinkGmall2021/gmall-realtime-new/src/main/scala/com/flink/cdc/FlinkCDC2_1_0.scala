package com.flink.cdc

import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions
import com.alibaba.ververica.cdc.debezium.{DebeziumSourceFunction, StringDebeziumDeserializationSchema}
import com.flink.function.CustomerDeserializer
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment


/**
 *  bin/flink savepoint job_id path
 *  bin/flink run -m -s -c jar  从savepoint启动
 */
object FlinkCDC2_1_0 {
  def main(args: Array[String]): Unit = {

    //1、获取执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    //1.1开启ck并指定状态后端   memory/fs/rocksdb
//    env.setStateBackend(new FsStateBackend("file:///Users/liufukang/data/finkcdc_ck/gmall_ck"))
//    env.enableCheckpointing(5000)
//    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
//    env.getCheckpointConfig.setCheckpointTimeout(10000)
//    env.getCheckpointConfig.setMaxConcurrentCheckpoints(2)
//    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(2000)

//    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(5,50000))

    //2、通过FlinkCDC构建sourceFuntion并读取数据
    val source: DebeziumSourceFunction[String] = MySQLSource.builder[String]()
      .hostname("hadoop100")
      .port(3306)
      .username("root")
      .password("123456")
      .databaseList("gmall_config")
      .tableList("gmall_config.table_process")
      .deserializer(new CustomerDeserializer)
      .startupOptions(StartupOptions.initial())
      .build()
    val mysqlSourceDS = env.addSource(source)

    //3.打印数据
    mysqlSourceDS.print()

    //4、启动任务
    env.execute("FlinkCDC")
  }
}
