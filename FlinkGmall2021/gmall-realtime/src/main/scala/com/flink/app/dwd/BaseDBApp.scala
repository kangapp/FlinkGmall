package com.flink.app.dwd

import com.alibaba.fastjson.JSON
import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction
import com.flink.function.CustomerDeserializer
import com.flink.util.MyKafkaUtil
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

object BaseDBApp {

  def main(args: Array[String]): Unit = {

    //TODO 1、获取执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    //1.1开启ck并指定状态后端   memory/fs/rocksdb
    //    env.setStateBackend(new FsStateBackend("file:///Users/liufukang/data/finkcdc_ck/gmall_ck"))
    //    env.enableCheckpointing(5000)
    //    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    //    env.getCheckpointConfig.setCheckpointTimeout(10000)
    //    env.getCheckpointConfig.setMaxConcurrentCheckpoints(2)
    //    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(2000)

//        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(5,5000))

    //TODO 2、消费kafka ods_base_db主题数据创建流
    val sourceTopic = "ods_base_db"
    val groupId = "base_db_app"
    val kafkaDS = env.addSource(MyKafkaUtil.getKafkaConsumer(sourceTopic,groupId))

    //TODO 3、将每行数据转换为JSON对象并过滤（delete）
    val jsonObjDS = kafkaDS.map(JSON.parseObject(_)).filter(item => "delete".equalsIgnoreCase(item.getString("type")))

    //TODO 4、使用FlinkCDC消费配置表并处理成   广播流
    val source:DebeziumSourceFunction[String] = MySQLSource.builder[String]()
      .hostname("localhost")
      .port(3306)
      .username("root")
      .password("123456")
      .databaseList("bigdata")
      .tableList("bigdata.table_process")
      .deserializer(new CustomerDeserializer)
      .startupOptions(StartupOptions.latest())
      .build()
    val tableProcessDS = env.addSource(source)
    val mapStateDescriptor = new MapStateDescriptor()
    tableProcessDS.broadcast()

    //TODO 5、连接主流和广播流

    //TODO 6、分流  处理数据  广播流数据，主流数据（根据广播流数据进行处理）

    //TODO 7、提取kafka流数据和HBase流数据

    //TODO 8、将kafka数据写入kafka主题，将HBase数据写入Phoneix表

    //TODO 9、启动任务
  }

}
