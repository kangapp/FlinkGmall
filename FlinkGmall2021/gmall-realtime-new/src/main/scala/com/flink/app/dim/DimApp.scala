package com.flink.app.dim

import com.flink.utils.MyKafkaUtil
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

object DimApp {

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


}
