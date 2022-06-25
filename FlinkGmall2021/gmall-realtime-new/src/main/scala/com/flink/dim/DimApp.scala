package com.flink.dim

import com.alibaba.fastjson.{JSON, JSONObject}
import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions
import com.alibaba.ververica.cdc.debezium.{DebeziumSourceFunction, StringDebeziumDeserializationSchema}
import com.flink.bean.TableProcess
import com.flink.function.{CustomerDeserializer, DimSinkFunction, TableProcessFunction}
import com.flink.utils.MyKafkaUtil
import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.{BroadcastConnectedStream, DataStream, OutputTag, StreamExecutionEnvironment}
import org.apache.flink.util.Collector

object DimApp {

  def main(args: Array[String]): Unit = {
    //TODO 1、获取执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    //  env.setStateBackend(new FsStateBackend("file:///Users/liufukang/data/finkcdc_ck/gmall_ck"))
    //  env.enableCheckpointing(5000)
    //  env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    //  env.getCheckpointConfig.setCheckpointTimeout(10000)
    //  env.getCheckpointConfig.setCheckpointStorage("")
    //  env.getCheckpointConfig.setMaxConcurrentCheckpoints(2)
    //  env.getCheckpointConfig.setMinPauseBetweenCheckpoints(2000)

    //TODO 2、读取kafka topic_db主题数据创建流
    val kafkaDS:DataStream[String] = env.addSource(MyKafkaUtil.getKafkaConsumer("topic_db","dim_app"))

    //TODO 3、过滤非JSON格式的数据，并将其写入侧输出流
    val dirtyDataTap = new OutputTag[String]("Dirty")
    val jsonObjDS: DataStream[JSONObject] = kafkaDS.process(new ProcessFunction[String, JSONObject] {
      override def processElement(i: String, context: ProcessFunction[String, JSONObject]#Context, collector: Collector[JSONObject]): Unit = {
        try {
          val jsonObject = JSON.parseObject(i)
          collector.collect(jsonObject)
        } catch {
          case e: Exception => {
            context.output(dirtyDataTap, i)
          }
        }
      }
    })

    val sideOutput: DataStream[String] = jsonObjDS.getSideOutput(dirtyDataTap)
    sideOutput.print("Dirty>>>>>>>>>>>>")

    //TODO 4、使用FlinkCDC读取MySQL中的配置信息
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
    mysqlSourceDS.print("source>>>>>>>>>>>>")

    //TODO 5、将配置信息流处理成广播流
    val mapStateDescriptor = new MapStateDescriptor[String,TableProcess]("map-state",Types.STRING,createTypeInformation[TableProcess])
    val broadcastStream = mysqlSourceDS.broadcast(mapStateDescriptor)

    //TODO 6、连接主流与广播流
    val connectedStream: BroadcastConnectedStream[JSONObject, String] = jsonObjDS.connect(broadcastStream)

    //TODO 7、根据广播流处理主流数据
    val hbaseDS: DataStream[JSONObject] = connectedStream.process(new TableProcessFunction(mapStateDescriptor))

    //TODO 8、将数据写出到phoenix中
    hbaseDS.addSink(new DimSinkFunction)

    //TODO 9、启动任务
    env.execute("DimApp")
  }


}
