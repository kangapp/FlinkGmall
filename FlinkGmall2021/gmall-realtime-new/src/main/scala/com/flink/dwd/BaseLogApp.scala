package com.flink.dwd

import com.alibaba.fastjson.{JSON, JSONObject}
import com.flink.utils.{DateFormatUtil, MyKafkaUtil}
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.{DataStream, OutputTag, StreamExecutionEnvironment}
import org.apache.flink.util.Collector

object BaseLogApp {

  def main(args: Array[String]): Unit = {

    //TODO 1、获取执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
//    env.setStateBackend(new FsStateBackend("file:///Users/liufukang/data/finkcdc_ck/gmall_ck"))
//    env.enableCheckpointing(5000)
//    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
//    env.getCheckpointConfig.setCheckpointTimeout(10000)
//    env.getCheckpointConfig.setCheckpointStorage("")
//    env.getCheckpointConfig.setMaxConcurrentCheckpoints(2)
//    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(2000)

    //TODO 2、读取kafka topic_log 主题的数据创建流
    val kafkaDS: DataStream[String] = env.addSource(MyKafkaUtil.getKafkaConsumer("topic_log",
      "base_log_app"))

    //TODO 3、将数据转化为JSON格式，并过滤掉非JSON格式的数据
    val dirtyTag: OutputTag[String] = OutputTag[String]("dirty")
    val cleanedDS: DataStream[JSONObject] = kafkaDS.process(new ProcessFunction[String, JSONObject] {
      override def processElement(i: String, context: ProcessFunction[String, JSONObject]#Context, collector: Collector[JSONObject]): Unit = {
        try {
          val jSONObject = JSON.parseObject(i)
          collector.collect(jSONObject)
        } catch {
          case exception: Exception => {
            context.output(dirtyTag, i)
          }
        }
      }
    })
    /**
     * 处理脏数据，保存到kafka
     */
    val dirtyDS: DataStream[String] = cleanedDS.getSideOutput(dirtyTag)
    dirtyDS.addSink(MyKafkaUtil.getKafkaProducer("dirty_data"))

    //TODO 4、使用状态编程做新老用户校验
    val keyedDS = cleanedDS.keyBy(_.getJSONObject("common").getString("mid"))
    val fixedDS: DataStream[JSONObject] = keyedDS.map(new RichMapFunction[JSONObject, JSONObject] {

      private var lastVisitDtState: ValueState[String] = _

      override def open(parameters: Configuration): Unit = {
        lastVisitDtState = getRuntimeContext.
          getState(new ValueStateDescriptor[String]("last-visit", classOf[String]))
      }

      override def map(in: JSONObject): JSONObject = {
        val isNew = in.getJSONObject("common").getString("is_new")
        val lastVisitDt = lastVisitDtState.value()
        val ts = in.getLong("ts")

        if ("1".equals(isNew)) {

          val curDt = DateFormatUtil.toDate(ts)

          if (lastVisitDt == null) {
            lastVisitDtState.update(curDt)
          } else if (!lastVisitDt.equals(curDt)) {
            in.getJSONObject("common").put("is_new", "0")
          }
        } else if (lastVisitDt == null) {
          val yesterday = DateFormatUtil.toDate(ts - 24 * 60 * 60 * 1000L)
          lastVisitDtState.update(yesterday)
        }

        in
      }
    })

    //TODO 5、使用侧输出流对数据进行分流处理
    val startTag = OutputTag[String]("start")
    val displayTag = OutputTag[String]("display")
    val actionTag = OutputTag[String]("action")
    val errorTag = OutputTag[String]("error")

    val separatedDS: DataStream[String] = fixedDS.process(new ProcessFunction[JSONObject, String] {
      override def processElement(i: JSONObject, context: ProcessFunction[JSONObject, String]#Context, collector: Collector[String]): Unit = {
        val jsonString = i.toJSONString

        val error = i.getString("error")
        if (error != null) {
          context.output(errorTag, jsonString)
        }

        val start = i.getString("start")
        if (start != null) {
          context.output(startTag, jsonString)
        } else {

          val page = i.getJSONObject("page");
          val common = i.getJSONObject("common");
          val ts = i.getLong("ts");

          val displays = i.getJSONArray("displays")
          if (displays != null && displays.size() > 0) {
            for (i <- 0 until displays.size()) {
              val display = displays.getJSONObject(i)
              display.put("page", page)
              display.put("ts", ts)
              display.put("common", common)

              context.output(displayTag, display.toJSONString)
            }
          }

          val actions = i.getJSONArray("actions")
          if (actions != null && actions.size() > 0) {
            for (i <- 0 until actions.size()) {
              val action = actions.getJSONObject(i)
              action.put("page", page)
              action.put("ts", ts)
              action.put("common", common)

              context.output(actionTag, action.toJSONString)
            }
          }

          i.remove("displays")
          i.remove("actions")
          collector.collect(i.toJSONString)
        }
      }
    })

    //TODO 6、提取各个数据流的数据
    val startDS = separatedDS.getSideOutput(startTag)
    val actionDS = separatedDS.getSideOutput(actionTag)
    val displayDS = separatedDS.getSideOutput(displayTag)
    val errorDS = separatedDS.getSideOutput(errorTag)

    //TODO 7、将数据输出到对应的kafka

    val page_topic = "dwd_traffic_page_log";
    val start_topic = "dwd_traffic_start_log";
    val display_topic = "dwd_traffic_display_log";
    val action_topic = "dwd_traffic_action_log";
    val error_topic = "dwd_traffic_error_log";

    separatedDS.addSink(MyKafkaUtil.getKafkaProducer(page_topic))
    startDS.addSink(MyKafkaUtil.getKafkaProducer(start_topic))
    displayDS.addSink(MyKafkaUtil.getKafkaProducer(display_topic))
    actionDS.addSink(MyKafkaUtil.getKafkaProducer(action_topic))
    errorDS.addSink(MyKafkaUtil.getKafkaProducer(error_topic))


    env.execute("baseLogApp")
  }
}
