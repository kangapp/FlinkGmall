package com.flink.app.dwd

import com.alibaba.fastjson.{JSON, JSONObject}
import com.flink.util.MyKafkaUtil
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.datastream.{DataStreamSource, SingleOutputStreamOperator}
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.configuration.Configuration
import org.apache.flink.util.Collector
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.OutputTag

object BaseLogApp {

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

    //    env.setRestartStrategy(RestartStrategies.fixedDelayRestart())

    //TODO 2、消费ods_base_log 主题创建流
    val sourceTopic = "ods_base_log"
    val group_id = "base_log"
    val kafkaDS:DataStreamSource[String] = env.addSource(MyKafkaUtil.getKafkaConsumer(sourceTopic,group_id))

    //TODO 3、将每行数据转换为JSON对象
    /**
     * 防止解析异常导致程序终止，使用process方法
     */
    //  kafkaDS.map(new MapFunction[String,JSONObject] {
    //    override def map(t: String): JSONObject = {
    //      JSON.parseObject(t)
    //    }
    //  })
    val outputTag = OutputTag[String]("Dirty")
    val jsonObjDS:SingleOutputStreamOperator[JSONObject] = kafkaDS.process(new ProcessFunction[String,JSONObject] {
      override def processElement(value: String, context: ProcessFunction[String, JSONObject]#Context, out: Collector[JSONObject]): Unit = {
        try {
          val jsonObject:JSONObject = JSON.parseObject(value)
          out.collect(jsonObject)
        } catch {
          case e:Exception => {
            //发生异常，将数据写入侧输出流
            e.printStackTrace()
            context.output(outputTag,value)
          }
        }
      }
    },createTypeInformation[JSONObject])

    //打印脏数据
    jsonObjDS.getSideOutput(outputTag).print("Dirty>>>>>>>>>>>>>>>>>>>>")

    //TODO 4、新老用户校验 状态编程
    val jsonObjWithNewFlagDS = jsonObjDS.keyBy((value: JSONObject) => value.getJSONObject("common").getString("mid"),Types.STRING)
      .map(new RichMapFunction[JSONObject,JSONObject] {
      private var valueState:ValueState[String] = _
      override def open(parameters: Configuration): Unit = {
        valueState = getRuntimeContext.getState(new ValueStateDescriptor[String]("value-state",Types.STRING))
      }
      override def map(value: JSONObject): JSONObject = {
        //获取数据中的"is_new标记"
        val isNew = value.getJSONObject("common").getString("is_new")

        if("1".equalsIgnoreCase(isNew)) {
          //获取状态数据
          val state = valueState.value()
          if(state != null){
            //修改isNew标记
            value.getJSONObject("common").put("is_new","0")
          } else{
            valueState.update("1")
          }
        }
        value
      }
    },createTypeInformation[JSONObject])

    //TODO 5、分流 侧输出流 页面：主流  启动：侧输出流  曝光：侧输出流
    val startOutputTag = new OutputTag[String]("start")
    val displayOutputTag = new OutputTag[String]("display")
    val pageDS = jsonObjWithNewFlagDS.process(new ProcessFunction[JSONObject,String] {
      override def processElement(input: JSONObject, context: ProcessFunction[JSONObject, String]#Context, collector: Collector[String]): Unit = {
        //获取启动日志字段
        val start = input.getString("start")
        if(start != null && start.length > 0) {
          //将数据写入启动日志侧输出流
          context.output(startOutputTag, input.toJSONString)
        } else{
          //将数据写入页面日志主流
          collector.collect(input.toJSONString)

          //取出数据中的曝光数据
          val displays = input.getJSONArray("displays")
          if(displays != null && displays.size() > 0) {
            val pageId = input.getJSONObject("page").getString("page_id")

            for(i <-  0 until displays.size()) {
              val display = displays.getJSONObject(i)

              //添加页面id
              display.put("page_id",pageId)
              context.output(displayOutputTag,display.toJSONString)
            }
          }
        }
      }
    },Types.STRING)

    //TODO 6、提取侧输出流
    val startDS = pageDS.getSideOutput(startOutputTag)
    val displayDS = pageDS.getSideOutput(displayOutputTag)

    //TODO 7、将三个流打印并输出到对应的kafka主题中
    startDS.print("Start>>>>>>>>>>>>>>>>>>>>")
    pageDS.print("Page>>>>>>>>>>>>>>>>>>>>>>")
    displayDS.print("Display>>>>>>>>>>>>>>>>")

    startDS.addSink(MyKafkaUtil.getKafkaProducer("dwd_start_log"))
    pageDS.addSink(MyKafkaUtil.getKafkaProducer("dwd_page_log"))
    displayDS.addSink(MyKafkaUtil.getKafkaProducer("dwd_display_log"))

    //TODO 8、启动任务
    env.execute("BaseLogApp")
  }
}
