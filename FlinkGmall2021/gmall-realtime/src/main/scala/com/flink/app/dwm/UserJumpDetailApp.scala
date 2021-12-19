package com.flink.app.dwm

import com.alibaba.fastjson.{JSON, JSONObject}
import com.flink.util.MyKafkaUtil
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.cep.{CEP, PatternSelectFunction, PatternTimeoutFunction}
import org.apache.flink.cep.pattern.Pattern
import org.apache.flink.cep.pattern.conditions.SimpleCondition
import org.apache.flink.streaming.api.scala.OutputTag
import org.apache.flink.streaming.api.windowing.time.Time

import java.time.Duration
import java.util

object UserJumpDetailApp {
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

    //    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(5,5000))

    //TODO 2、读取kafka主题的数据创建流
    val groupId = "user_jump_detail_app"
    val sourceTopic = "dwd_page_log"
    val sinkTopic = "dwm_user_jump_detail"
    val kafkaDS = env.addSource(MyKafkaUtil.getKafkaConsumer(sourceTopic,groupId))

    //TODO 3、将每行数据转换为JSON对象并提取时间戳生成watermark
    val jsonObjDS = kafkaDS.map(JSON.parseObject(_)).assignTimestampsAndWatermarks(
      WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(2))
        .withTimestampAssigner(new SerializableTimestampAssigner[JSONObject] {
          override def extractTimestamp(value: JSONObject, l: Long): Long = {
            value.getLong("ts")
          }
        })
    )

    //TODO 4、定义模式序列
    val pattern = Pattern.begin[JSONObject]("start").where(new SimpleCondition[JSONObject] {
      override def filter(value: JSONObject): Boolean = {
        val lastPageId = value.getJSONObject("page").getString("last_page_id")
        lastPageId == null || lastPageId.length == 0
      }
    }).next("next").where(new SimpleCondition[JSONObject] {
      override def filter(value: JSONObject): Boolean = {
        val lastPageId = value.getJSONObject("page").getString("last_page_id")
        lastPageId == null || lastPageId.length == 0
      }
    }).within(Time.seconds(10))

    //TODO 4.1 使用循环模式 定义模式序列
//    Pattern.begin[JSONObject]("start").where(new SimpleCondition[JSONObject] {
//      override def filter(value: JSONObject): Boolean = {
//        val lastPageId = value.getJSONObject("page").getString("last_page_id")
//        lastPageId == null || lastPageId.length == 0
//      }
//    }).times(2)
//      .consecutive()  //指定严格近邻
//      .within(Time.seconds(10))

    //TODO 5、将模式序列作用到流上
    val patternStream = CEP.pattern(jsonObjDS.keyBy((value:JSONObject) => value.getJSONObject("common").getString("mid"))
      ,pattern)

    //TODO 6、提取匹配上的和超时事件
    val timeOutTag = new OutputTag[JSONObject]("timeout")
    val selectDS = patternStream.select(timeOutTag,new PatternTimeoutFunction[JSONObject, JSONObject] {
      override def timeout(map: util.Map[String, util.List[JSONObject]], l: Long): JSONObject = map.get("start").get(0)
    },new PatternSelectFunction[JSONObject, JSONObject] {
      override def select(map: util.Map[String, util.List[JSONObject]]): JSONObject = map.get("start").get(0)
    })
    val timeOutDS = selectDS.getSideOutput(timeOutTag)

    //TODO 7、UNION两种事件
    val unionDS = selectDS.union(timeOutDS)

    //TODO 8、将数据写入到kafka
    unionDS.print("union>>>>>>>>>>>>>>>>")
    unionDS.map(_.toJSONString).addSink(MyKafkaUtil.getKafkaProducer(sinkTopic))

    //TODO 9、启动任务
    env.execute("UserJumpDetailApp")
  }
}
