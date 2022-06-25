package com.flink.bean.dws

import com.alibaba.fastjson.JSON
import com.flink.bean.VisitorStats
import com.flink.util.{ClickHouseUtil, DateTimeUtil, MyKafkaUtil}
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import java.lang
import java.time.Duration
import java.util.Date

object VisitorStatsApp {
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

    //TODO 2、从kafka的 pv、uv、跳转明细主题中获取数据，并转换为JavaBean对象
    val groupId: String = "visitor_stats_app"
    val pageViewSourceTopic: String = "dwd_page_log"
    val uniqueVisitSourceTopic: String = "dwm_unique_visit"
    val userJumpDetailSourceTopic: String = "dwm_user_jump_detail"

    val pvDS = env.addSource(MyKafkaUtil.getKafkaConsumer(pageViewSourceTopic, groupId))
    val uvDS = env.addSource(MyKafkaUtil.getKafkaConsumer(uniqueVisitSourceTopic, groupId))
    val ujDS = env.addSource(MyKafkaUtil.getKafkaConsumer(userJumpDetailSourceTopic, groupId))

    val visitStatsWithPvDS = pvDS.map(line => {
      val jsonObject = JSON.parseObject(line)
      val last_page_id = jsonObject.getString("last_page_id")
      val sv = if (last_page_id == null || last_page_id.length <= 0) 1L else 0L
      new VisitorStats("","",
        jsonObject.getJSONObject("common").getString("vc"),
        jsonObject.getJSONObject("common").getString("ch"),
        jsonObject.getJSONObject("common").getString("ar"),
        jsonObject.getJSONObject("common").getString("is_new"),
        0L, 1L, sv, 0L, jsonObject.getJSONObject("page").getLong("during_time"),
        jsonObject.getLong("ts"))
    })
    val visitStatsWithUvDS = uvDS.map(line => {
      val jsonObject = JSON.parseObject(line)
      new VisitorStats("","",
        jsonObject.getJSONObject("common").getString("vc"),
        jsonObject.getJSONObject("common").getString("ch"),
        jsonObject.getJSONObject("common").getString("ar"),
        jsonObject.getJSONObject("common").getString("is_new"),
        1L, 0L, 0L, 0L, 0L,
        jsonObject.getLong("ts"))
    })
    val visitStatsWithUjDS = ujDS.map(line => {
      val jsonObject = JSON.parseObject(line)
      new VisitorStats("","",
        jsonObject.getJSONObject("common").getString("vc"),
        jsonObject.getJSONObject("common").getString("ch"),
        jsonObject.getJSONObject("common").getString("ar"),
        jsonObject.getJSONObject("common").getString("is_new"),
        0L, 0L, 0L, 1L, 0L,
        jsonObject.getLong("ts"))
    })

    //TODO 3、union3个流
    val unionDS = visitStatsWithPvDS.union(visitStatsWithUvDS,visitStatsWithUjDS)

    //TODO 4、提取时间戳生成WaterMark
    val visitorStatsWithWMDS = unionDS.assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(12))
    .withTimestampAssigner(new SerializableTimestampAssigner[VisitorStats] {
      override def extractTimestamp(t: VisitorStats, l: Long): Long = {
        t.getTs
      }
    }))

    //TODO 5、按照维度信息分组
    val keyDS = visitorStatsWithWMDS.keyBy(new KeySelector[VisitorStats, (String, String, String, String)] {
      override def getKey(item: VisitorStats): (String, String, String, String) = (item.getVc, item.getCh, item.getAr, item.getIs_new)
    })

    //TODO 6、开窗聚合 10s的滚动窗口
    val windowDS = keyDS.window(TumblingEventTimeWindows.of(Time.seconds(10)))
    val resultDS = windowDS.reduce(
      (r1: VisitorStats, r2: VisitorStats) => {
        r1.setUv_ct(r1.getUv_ct + r2.getUv_ct)
        r1.setPv_ct(r1.getPv_ct + r2.getPv_ct)
        r1.setSv_ct(r1.getSv_ct + r2.getSv_ct)
        r1.setUj_ct(r1.getUj_ct + r2.getUj_ct)
        r1.setDur_sum(r1.getDur_sum + r2.getDur_sum)
        r1
      }, new ProcessWindowFunction[VisitorStats, VisitorStats, (String, String, String, String), TimeWindow] {
        override def process(key: (String, String, String, String), context: ProcessWindowFunction[VisitorStats, VisitorStats, (String, String, String, String), TimeWindow]#Context, iterable: lang.Iterable[VisitorStats], collector: Collector[VisitorStats]): Unit = {
          val start = context.window().getStart
          val end = context.window().getEnd

          val visitorStats = iterable.iterator().next()

          //补充窗口信息
          visitorStats.setStt(DateTimeUtil.toYMDhms(new Date(start)))
          visitorStats.setEdt(DateTimeUtil.toYMDhms(new Date(end)))

          collector.collect(visitorStats)
        }
      })

    //TODO 7、数据写入ClickHouse
    resultDS.print("resultDS>>>>>>>>>>>>>>>>>>>>>>>")
    resultDS.addSink(ClickHouseUtil.getSink("insert into visitor_stats_2021 values(?,?,?,?,?,?,?,?,?,?,?,?)"))

    //TODO 8、启动任务
    env.execute("VisitorStatsApp")
  }
}
