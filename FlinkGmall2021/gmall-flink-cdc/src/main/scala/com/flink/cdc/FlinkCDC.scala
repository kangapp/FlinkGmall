package com.flink.cdc

import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions
import com.alibaba.ververica.cdc.debezium.{DebeziumSourceFunction, StringDebeziumDeserializationSchema}
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

object FlinkCDC {
  def main(args: Array[String]): Unit = {

    //1、获取执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    //2、通过FlinkCDC构建sourceFuntion并读取数据
    val source:DebeziumSourceFunction[String] = MySQLSource.builder[String]()
      .hostname("localhost")
      .port(3306)
      .username("root")
      .password("123456")
      .databaseList("flink")
//      .tableList()
      .deserializer(new StringDebeziumDeserializationSchema)
      .startupOptions(StartupOptions.latest())
      .build()
    val dataStream = env.addSource(source)

    //3.打印数据
    dataStream.print()

    //4、启动任务
    env.execute("FlinkCDC")
  }
}
