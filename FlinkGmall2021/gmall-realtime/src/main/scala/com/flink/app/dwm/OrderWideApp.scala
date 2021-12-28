package com.flink.app.dwm

import cn.hutool.core.date.DateUtil
import com.alibaba.fastjson.serializer.SerializerFeature
import com.alibaba.fastjson.{JSON, JSONObject}
import com.flink.bean.{OrderDetail, OrderInfo, OrderWide}
import com.flink.function.DimAsyncFunction
import com.flink.util.MyKafkaUtil
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.streaming.api.datastream.{AsyncDataStream, SingleOutputStreamOperator}
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

import java.text.SimpleDateFormat
import java.util.concurrent.TimeUnit

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
    val orderInfoDS = env.addSource(MyKafkaUtil.getKafkaConsumer(orderInfoSourceTopic,groupId))
      .map(line => {
        val orderInfo:OrderInfo = JSON.parseObject(line, classOf[OrderInfo])

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
        val orderDetail:OrderDetail = JSON.parseObject(line, classOf[OrderDetail])
        val createTime = orderDetail.getCreate_time

        val simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        orderDetail.setCreate_ts(simpleDateFormat.parse(createTime).getTime)
        orderDetail
      }).assignTimestampsAndWatermarks(WatermarkStrategy.forMonotonousTimestamps()
      .withTimestampAssigner(new SerializableTimestampAssigner[OrderDetail] {
        override def extractTimestamp(element: OrderDetail, l: Long): Long = element.getCreate_ts
      }))

    //TODO 3、双流JOIN
    val orderWideWithoutDimDS:SingleOutputStreamOperator[OrderWide] = orderInfoDS.keyBy((orderInfo:OrderInfo) => orderInfo.getId)
      .intervalJoin(orderDetailDS.keyBy((orderDetail:OrderDetail) => orderDetail.getOrder_id))
      .between(Time.seconds(-5), Time.seconds(5))
      .process((in1: OrderInfo, in2: OrderDetail, context: ProcessJoinFunction[OrderInfo, OrderDetail, OrderWide]#Context, collector: Collector[OrderWide]) => {
        collector.collect(new OrderWide(in1, in2))
      })

    //TODO 4、关联维度信息
//    orderWideWithoutDimDS.map(orderWide => {
//      val user_id = orderWide.getUser_id
//    })

    //4.1 关联用户维度
    val orderWideWithUserDS = AsyncDataStream.unorderedWait(orderWideWithoutDimDS, new DimAsyncFunction[OrderWide]("DIM_USER_INFO") {
      override def getKey(orderWide: OrderWide): String = orderWide.getUser_id.toString

      override def join(orderWide: OrderWide, dimInfo: JSONObject): Unit = {
        orderWide.setUser_gender(dimInfo.getString("gender"))
        val birthday = dimInfo.getString("birthday")
        orderWide.setUser_age(DateUtil.ageOfNow(birthday))
      }
    }, 60, TimeUnit.SECONDS)

    orderWideWithUserDS.print("orderWideWithUserDS>>>>>>>>>>>>>>>>>>")

    //4.2 关联地区维度
    val orderWideWithProvinceDS = AsyncDataStream.unorderedWait(orderWideWithUserDS, new DimAsyncFunction[OrderWide]("DIM_BASE_PROVINCE") {
      override def getKey(orderWide: OrderWide): String = orderWide.getProvince_id.toString

      override def join(orderWide: OrderWide, dimInfo: JSONObject): Unit = {
        orderWide.setProvince_name(dimInfo.getString("name"))
        orderWide.setProvince_area_code(dimInfo.getString("areaCode"))
        orderWide.setProvince_iso_code(dimInfo.getString("isoCode"))
        orderWide.setProvince_3166_2_code(dimInfo.getString("iso31662"))
      }
    }, 60, TimeUnit.SECONDS)
    orderWideWithProvinceDS.print("orderWideWithProvinceDS>>>>>>>>>>>>>>>>>")

    //4.3 关联sku维度
    val orderWideWithSkuDS = AsyncDataStream.unorderedWait(orderWideWithProvinceDS, new DimAsyncFunction[OrderWide]("DIM_SKU_INFO") {
      override def getKey(orderWide: OrderWide): String = orderWide.getSku_id.toString

      override def join(orderWide: OrderWide, dimInfo: JSONObject): Unit = {
        orderWide.setSku_name(dimInfo.getString("skuName"))
        orderWide.setCategory3_id(dimInfo.getLong("category3Id"))
        orderWide.setSpu_id(dimInfo.getLong("spuId"))
        orderWide.setTm_id(dimInfo.getLong("tmId"))
      }
    }, 60, TimeUnit.SECONDS)
    orderWideWithSkuDS.print("orderWideWithSkuDS>>>>>>>>>>>>>>>>>>>")

    //4.4 关联spu维度
    val orderWideWithSpuDS = AsyncDataStream.unorderedWait(orderWideWithSkuDS, new DimAsyncFunction[OrderWide]("DIM_SPU_INFO") {
      override def getKey(orderWide: OrderWide): String = orderWide.getSpu_id.toString

      override def join(orderWide: OrderWide, dimInfo: JSONObject): Unit = {
        orderWide.setSpu_name(dimInfo.getString("spuName"))
      }
    }, 60, TimeUnit.SECONDS)
    orderWideWithSpuDS.print("orderWideWithSpuDS>>>>>>>>>>>>>>>>>>>>")

    //4.5 关联品牌维度
    val orderWideWithTmDS = AsyncDataStream.unorderedWait(orderWideWithSpuDS, new DimAsyncFunction[OrderWide]("DIM_BASE_TRADEMARK") {
      override def getKey(orderWide: OrderWide): String = orderWide.getTm_id.toString

      override def join(orderWide: OrderWide, dimInfo: JSONObject): Unit = {
        orderWide.setTm_name(dimInfo.getString("tmName"))
      }
    }, 60, TimeUnit.SECONDS)
    orderWideWithTmDS.print("orderWideWithTmDS>>>>>>>>>>>>>>>>>>>")

    //4.6 关联品类维度
    val orderWideWithCategory3DS = AsyncDataStream.unorderedWait(orderWideWithTmDS, new DimAsyncFunction[OrderWide]("DIM_BASE_CATEGORY3") {
      override def getKey(orderWide: OrderWide): String = orderWide.getCategory3_id.toString

      override def join(orderWide: OrderWide, dimInfo: JSONObject): Unit = {
        orderWide.setCategory3_name(dimInfo.getString("name"))
      }
    }, 60, TimeUnit.SECONDS)
    orderWideWithCategory3DS.print("orderWideWithCategory3DS>>>>>>>>>>>>>>>>>>>")

    //TODO 5、将数据写入到kafka
    orderWideWithCategory3DS.map(JSON.toJSONString(_, SerializerFeature.EMPTY: _*)).addSink(MyKafkaUtil.getKafkaProducer(orderWideSinkTopic))

    //TODO 6、启动任务
    env.execute("OrderWideApp")
  }
}
