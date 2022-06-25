package com.flink.bean.dws;

import com.flink.bean.udf.KeywordUDTF;
import com.flink.bean.KeywordStats;
import com.flink.common.GmallConstant;
import com.flink.util.ClickHouseUtil;
import com.flink.util.MyKafkaUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class KeywordStatsApp {
    public static void main(String[] args) throws Exception {
        //TODO 1、获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置并行度
        env.setParallelism(1);
        //检查点 CK 相关设置
//        env.enableCheckpointing(5000, CheckpointingMode.AT_LEAST_ONCE);
//        env.getCheckpointConfig().setCheckpointTimeout(60000);
//        StateBackend fsStateBackend = new FsStateBackend("hdfs://hadoop102:8020/gmall/flink/checkpoint/ProductStatsApp");
//        env.setStateBackend(fsStateBackend);
//
//        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(5,5000));

        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        //TODO 2.注册自定义函数
        tableEnv.createTemporarySystemFunction("ik_analyze", KeywordUDTF.class);

        //TODO 3.将数据源定义为动态表
        String groupId = "keyword_stats_app";
        String pageViewSourceTopic ="dwd_page_log";
        tableEnv.executeSql("CREATE TABLE page_view " +
                "(common MAP<STRING,STRING>, " +
                "page MAP<STRING,STRING>,ts BIGINT, " +
                "rowtime AS TO_TIMESTAMP(FROM_UNIXTIME(ts/1000, 'yyyy-MM-dd HH:mm:ss')) ," +
                "WATERMARK FOR rowtime AS rowtime - INTERVAL '2' SECOND) " +
                "WITH ("+ MyKafkaUtil.getKafkaDDL(pageViewSourceTopic,groupId)+")");

        //TODO 4.过滤数据
        Table fullwordView = tableEnv.sqlQuery("select page['item'] fullword ," +
                "rowtime from page_view " +
                "where page['page_id']='good_list' " +
                "and page['item'] IS NOT NULL ");

        //TODO 5.利用 udtf 将数据拆分
        Table keywordView = tableEnv.sqlQuery("select keyword,rowtime from " + fullwordView + " ," +
                " LATERAL TABLE(ik_analyze(fullword)) as T(keyword)");

        //TODO 6.根据各个关键词出现次数进行 ct
        Table keywordStatsSearch = tableEnv.sqlQuery("select keyword,count(*) ct, '" +
                        GmallConstant.KEYWORD_SEARCH + "' source ," +
                        "DATE_FORMAT(TUMBLE_START(rowtime, INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') stt," +
                        "DATE_FORMAT(TUMBLE_END(rowtime, INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') edt," +
                        "UNIX_TIMESTAMP()*1000 ts from "+keywordView +
                        " GROUP BY TUMBLE(rowtime, INTERVAL '10' SECOND ),keyword");

        //TODO 7.转换为数据流
        DataStream<KeywordStats> keywordStatsSearchDataStream =
                tableEnv.toAppendStream(keywordStatsSearch, KeywordStats.class);

        //TODO 8.写入到 ClickHouse
        keywordStatsSearchDataStream.addSink(
                ClickHouseUtil.getSink(
                        "insert into keyword_stats(keyword,ct,source,stt,edt,ts) " +
                                " values(?,?,?,?,?,?)"));

        env.execute("KeywordStatsApp");
    }
}
