package com.flink.app.dwm

import com.alibaba.fastjson.JSON
import com.flink.bean.{OrderDetail, OrderInfo, OrderWide}
import com.flink.util.MyKafkaUtil
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

import java.text.SimpleDateFormat

object OrderWideApp {

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

    //TODO 2、读取kafka主题数据，并转换为JavaBean对象  提取时间戳生成watermark
    val orderInfoSourceTopic: String = "dwd_order_info"
    val orderDetailSourceTopic: String = "dwd_order_detail"
    val orderWideSinkTopic: String = "dwm_order_wide"
    val groupId: String = "order_wide_group"
    val orderInfoDS = env.addSource(MyKafkaUtil.getKafkaConsumer(orderDetailSourceTopic,groupId))
      .map(line => {
        val orderInfo:OrderInfo = JSON.parseObject(line, Class[OrderInfo])

        val createTime = orderInfo.getCreate_time
        val dateTimeArr = createTime.split(" ")
        orderInfo.setCreate_date(dateTimeArr(0))
        orderInfo.setCreate_hour(dateTimeArr(1))

        val simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        orderInfo.setCreate_ts(simpleDateFormat.parse(createTime).getTime)
        orderInfo
      }).assignTimestampsAndWatermarks(WatermarkStrategy.forMonotonousTimestamps()
      .withTimestampAssigner(new SerializableTimestampAssigner[OrderInfo] {
        override def extractTimestamp(element: OrderInfo, l: Long): Long = element.getCreate_ts
      }))

    val orderDetailDS = env.addSource(MyKafkaUtil.getKafkaConsumer(orderDetailSourceTopic,groupId))
      .map(line => {
        val orderDetail:OrderDetail = JSON.parseObject(line, Class[OrderDetail])
        val createTime = orderDetail.getCreate_time

        val simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        orderDetail.setCreate_ts(simpleDateFormat.parse(createTime).getTime)
        orderDetail
      }).assignTimestampsAndWatermarks(WatermarkStrategy.forMonotonousTimestamps()
      .withTimestampAssigner(new SerializableTimestampAssigner[OrderDetail] {
        override def extractTimestamp(element: OrderDetail, l: Long): Long = element.getCreate_ts
      }))

    //TODO 3、双流JOIN
    val orderWideWithoutDimDS = orderInfoDS.keyBy((orderInfo:OrderInfo) => orderInfo.getId)
      .intervalJoin(orderDetailDS.keyBy((orderDetail:OrderDetail) => orderDetail.getOrder_id))
      .between(Time.seconds(-5), Time.seconds(5))
      .process(new ProcessJoinFunction[OrderInfo, OrderDetail, OrderWide] {
        override def processElement(in1: OrderInfo, in2: OrderDetail, context: ProcessJoinFunction[OrderInfo, OrderDetail, OrderWide]#Context, collector: Collector[OrderWide]): Unit = {
          collector.collect(new OrderWide(in1, in2))
        }
      })

    orderWideWithoutDimDS.print("orderWideWithoutDinDs>>>>>>>>>>>>>>>")

    //TODO 4、关联维度信息

    //TODO 5、将数据写入到kafka

    //TODO 6、启动任务
    env.execute("OrderWideApp")
  }
}
