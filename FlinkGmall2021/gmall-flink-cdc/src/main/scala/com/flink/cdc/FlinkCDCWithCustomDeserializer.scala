package com.flink.cdc

import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions
import com.alibaba.ververica.cdc.debezium.{DebeziumSourceFunction, StringDebeziumDeserializationSchema}
import com.util.CustomerDeserializer
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

/**
 *  bin/flink savepoint job_id path
 *  bin/flink run -m -s -c jar  从savepoint启动
 */
object FlinkCDCWithCustomDeserializer {
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
    val source:DebeziumSourceFunction[String] = MySQLSource.builder[String]()
      .hostname("localhost")
      .port(3306)
      .username("root")
      .password("123456")
      .databaseList("flink")
//      .tableList()
      .deserializer(new CustomerDeserializer)
      .startupOptions(StartupOptions.initial())
      .build()
    val dataStream = env.addSource(source)

    //3.打印数据
    dataStream.print()

    //4、启动任务
    env.execute("FlinkCDCWithCustomDeserializer")
  }
}
