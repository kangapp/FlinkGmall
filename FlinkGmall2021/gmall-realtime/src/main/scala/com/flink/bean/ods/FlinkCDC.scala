package com.flink.bean.ods

import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction
import com.flink.function.CustomerDeserializer
import com.flink.util.MyKafkaUtil
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

object FlinkCDC {
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
      .deserializer(new CustomerDeserializer)
      .startupOptions(StartupOptions.initial())
      .build()
    val dataStream = env.addSource(source)

    //3.打印数据并将数据写入kafka
    dataStream.print()
    val sinkTopic:String = "ods_base_db"
    dataStream.addSink(MyKafkaUtil.getKafkaProducer(sinkTopic))

    //4、启动任务
    env.execute("FlinkCDCWithCustomDeserializer")
  }
}
