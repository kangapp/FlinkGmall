package com.flink.bean.dwm

import com.alibaba.fastjson.{JSON, JSONObject}
import com.flink.util.MyKafkaUtil
import org.apache.flink.api.common.functions.RichFilterFunction
import org.apache.flink.api.common.state.{StateTtlConfig, ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.time.Time
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.KeyedStream

import java.text.SimpleDateFormat

object UniqueVisitApp {
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

    //TODO 2、读取kafka dwd_page_log 主题的数据
    val groupId = "unique_visit_app"
    val sourceTopic = "dwd_page_log"
    val sinkTopic = "dwm_unique_visit"
    val kafkaDS = env.addSource(MyKafkaUtil.getKafkaConsumer(sourceTopic,groupId))

    //TODO 3、将每行数据转换为JSON对象
    val jsonObjDS = kafkaDS.map(JSON.parseObject(_))

    //TODO 4、过滤数据 状态编程 只保留每个mid每天第一次登录的数据
    val keyedStream = jsonObjDS.keyBy((value:JSONObject) => value.getJSONObject("common").getString("mid"))
    val uvDS = keyedStream.filter(new RichFilterFunction[JSONObject] {

      private var dateState:ValueState[String] = _
      private var simpleDateFormat:SimpleDateFormat = _

      override def open(parameters: Configuration): Unit = {
        val valueStateDescriptor = new ValueStateDescriptor[String]("date-state", Types.STRING)
        //设置状态的超时时间及更新时间的方式
        val stateTtlConfig = new StateTtlConfig
                  .Builder(Time.hours(24))
                  .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                  .build()
        valueStateDescriptor.enableTimeToLive(stateTtlConfig)
        dateState = getRuntimeContext.getState(valueStateDescriptor)

        simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
      }
      override def filter(value: JSONObject): Boolean = {

        //取出上一跳页面信息
        val lastPageId = value.getJSONObject("page").getString("last_page_id")
        if (lastPageId == null || lastPageId.length <= 0) {
          //取出状态数据
          val lastDate = dateState.value()
          //取出今天的日期
          val curDate = simpleDateFormat.format(value.getLong("ts"))
          if (!curDate.equals(lastDate)) {
            dateState.update(curDate)
            true
          } else false
        } else false
      }
    })

    //TODO 5、将数据写入kafka
    uvDS.print("uvDS>>>>>>>>>>>>>>>>>>>>>")
    uvDS.map(_.toJSONString).addSink(MyKafkaUtil.getKafkaProducer(sinkTopic))

    //TODO 6、启动任务
    env.execute("UniqueVisitApp")
  }
}
