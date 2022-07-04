package com.flink.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONAware;
import com.alibaba.fastjson.JSONObject;
import com.flink.utils.DateFormatUtil;
import com.flink.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

public class DwdTrafficUniqueVisitorDetail {
    public static void main(String[] args) throws Exception {

        //TODO 1、环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //TODO 2、状态后端设置
//        env.enableCheckpointing(3000L, CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setCheckpointTimeout(30 * 1000L);
//        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000L);
//        env.getCheckpointConfig().enableExternalizedCheckpoints(
//                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
//        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.days(1), Time.minutes(1)));
//        env.setStateBackend(new HashMapStateBackend());
//        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop100:8020/ck");
//        System.setProperty("HADOOP_USER_NAME", "root");

        // TODO 3、从 kafka dwd_traffic_page_log 主题读取日志数据，封装为流
        String topic = "dwd_traffic_page_log";
        String groupId = "dwd_traffic_user_jump_detail";
        FlinkKafkaConsumer<String> kafkaConsumer = MyKafkaUtil.getKafkaConsumer(topic, groupId);
        DataStreamSource<String> pageLog = env.addSource(kafkaConsumer);

        // TODO 4、转化成JSONObject，并过滤last_page_id不为null的数据
        SingleOutputStreamOperator<JSONObject> mapDS = pageLog.map(JSON::parseObject);
        SingleOutputStreamOperator<JSONObject> firstPageDS = mapDS.filter(jsonObject -> jsonObject.getJSONObject("page")
                .getString("last_page_id") == null);

        //TODO 5、按照mid分组
        KeyedStream<JSONObject, String> keyedDS = firstPageDS.keyBy(jsonObject ->
                jsonObject.getJSONObject("common").getString("mid"));

        //TODO 6、通过状态编程过滤独立访客记录
        SingleOutputStreamOperator<JSONObject> filterDS = keyedDS.filter(new RichFilterFunction<JSONObject>() {

            private ValueState<String> lastVisitDt;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                ValueStateDescriptor<String> valueStateDescriptor = new ValueStateDescriptor<>("last_visit_dt", String.class);
                valueStateDescriptor.enableTimeToLive(
                        StateTtlConfig.newBuilder(Time.days(1L))
                                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                                .build()
                );
                lastVisitDt = getRuntimeContext().getState(valueStateDescriptor);
            }

            @Override
            public boolean filter(JSONObject jsonObject) throws Exception {
                String visitDt = DateFormatUtil.toDate(jsonObject.getLong("ts"));
                String lastDt = lastVisitDt.value();
                if (lastDt == null || !lastDt.equals(visitDt)) {
                    lastVisitDt.update(visitDt);
                    return true;
                }
                return false;
            }
        });

        //TODO 7、将独立访客数据写入 Kafka dwd_traffic_unique_visitor_detail 主题
        String targetTopic = "dwd_traffic_unique_visitor_detail";
        FlinkKafkaProducer<String> kafkaProducer = MyKafkaUtil.getKafkaProducer(targetTopic);
        filterDS.map(JSONAware::toJSONString).addSink(kafkaProducer);


        //TODO 8、启动任务
        env.execute("DwdTrafficUniqueVisitorDetail");
    }
}
