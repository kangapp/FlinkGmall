package com.flink.bean.dwm

import com.alibaba.fastjson.serializer.SerializerFeature
import com.alibaba.fastjson.{JSON, JSONObject}
import com.flink.bean.{OrderWide, PaymentInfo, PaymentWide}
import com.flink.util.MyKafkaUtil
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

import java.text.SimpleDateFormat

object PaymentWideApp {
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

    //TODO 2、读取kafka主题的数据创建流 并转换为JavaBean对象 提取时间戳生成waterMark
    val groupId: String = "payment_wide_group"
    val paymentInfoSourceTopic: String = "dwd_payment_info"
    val orderWideSourceTopic: String = "dwm_order_wide"
    val paymentWideSinkTopic: String = "dwm_payment_wide"

    val orderWideDS = env.addSource(MyKafkaUtil.getKafkaConsumer(orderWideSourceTopic, groupId))
      .map(line => JSON.parseObject(line, classOf[OrderWide]))
      .assignTimestampsAndWatermarks(WatermarkStrategy.forMonotonousTimestamps()
      .withTimestampAssigner(new SerializableTimestampAssigner[OrderWide] {
        override def extractTimestamp(t: OrderWide, l: Long): Long = {
          val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
          try {
            sdf.parse(t.getCreate_time).getTime
          } catch {
            case ex: Exception => {
              l
            }
          }
        }
      }))

    val paymentInfoDS = env.addSource(MyKafkaUtil.getKafkaConsumer(paymentInfoSourceTopic, groupId))
      .map(line => JSON.parseObject(line, classOf[PaymentInfo]))
      .assignTimestampsAndWatermarks(WatermarkStrategy.forMonotonousTimestamps()
        .withTimestampAssigner(new SerializableTimestampAssigner[PaymentInfo] {
          override def extractTimestamp(t: PaymentInfo, l: Long): Long = {
            // 线程不安全，必须在方法里面创建对象
            val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
            try {
              sdf.parse(t.getCreate_time).getTime
            } catch {
              case ex: Exception => {
                l
              }
            }
          }
        }))

    //TODO 3、双流Join
    val paymentWideDS = paymentInfoDS.keyBy(_.getOrder_id)
      .intervalJoin(orderWideDS.keyBy(_.getOrder_id))
      .between(Time.minutes(-15), Time.seconds(5))
      .process(new ProcessJoinFunction[PaymentInfo, OrderWide, PaymentWide] {
        override def processElement(in1: PaymentInfo, in2: OrderWide, context: ProcessJoinFunction[PaymentInfo, OrderWide, PaymentWide]#Context, collector: Collector[PaymentWide]): Unit = {
          collector.collect(new PaymentWide(in1, in2))
        }
      })

    //TODO 4、将数据写入kafka
    paymentWideDS.print("paymentWideDS>>>>>>>>>>>>>>>>>>")
    paymentWideDS.map(JSON.toJSONString(_, SerializerFeature.EMPTY: _*)).addSink(MyKafkaUtil.getKafkaProducer(paymentWideSinkTopic))

    //TODO 5、启动任务
    env.execute("PaymentWideApp")
  }
}
