package com.flink.bean.dws;

import com.flink.bean.ProvinceStats;
import com.flink.util.ClickHouseUtil;
import com.flink.util.MyKafkaUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class ProvinceStatsSqlApp {

    public static void main(String[] args) throws Exception {
        //TODO 1、获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置并行度
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        //检查点 CK 相关设置
//        env.enableCheckpointing(5000, CheckpointingMode.AT_LEAST_ONCE);
//        env.getCheckpointConfig().setCheckpointTimeout(60000);
//        StateBackend fsStateBackend = new FsStateBackend("hdfs://hadoop102:8020/gmall/flink/checkpoint/ProductStatsApp");
//        env.setStateBackend(fsStateBackend);
//
//        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(5,5000));

        //TODO 2、使用DDL创建表 提取时间生成waterMark
        String groupId = "province_stats";
        String orderWideTopic = "dwm_order_wide";
        tableEnv.executeSql("CREATE TABLE ORDER_WIDE (province_id BIGINT, " +
                        "province_name STRING,province_area_code STRING," +
                        "province_iso_code STRING," +
                        "province_3166_2_code STRING,order_id STRING, " +
                        "total_amount DOUBLE," +
                        "create_time STRING," +
                        "rowtime AS TO_TIMESTAMP(create_time) ," +
                        "WATERMARK FOR rowtime AS rowtime)" +
                        " WITH (" + MyKafkaUtil.getKafkaDDL(orderWideTopic, groupId) + ")");

        //TODO 3、查询数据 分组、开窗、聚合
        Table provinceStateTable = tableEnv.sqlQuery("select " +
                        "DATE_FORMAT(TUMBLE_START(rowtime, INTERVAL '10' SECOND ),'yyyy-MM-dd HH:mm:ss') stt, " +
                        "DATE_FORMAT(TUMBLE_END(rowtime, INTERVAL '10' SECOND ),'yyyy-MM-dd HH:mm:ss') edt , " +
                        "province_id," +
                        "province_name," +
                        "province_area_code," +
                        "province_iso_code," +
                        "province_3166_2_code," +
                        "COUNT( DISTINCT order_id) order_count, " +
                        "sum(total_amount) order_amount," +
                        "UNIX_TIMESTAMP()*1000 ts "+
                        "from ORDER_WIDE group by TUMBLE(rowtime, INTERVAL '10' SECOND )," +
                        "province_id,province_name,province_area_code,province_iso_code,province_3166_2_code ");

        //TODO 4、将动态表转换为流
        DataStream<ProvinceStats> provinceStatsDataStream = tableEnv.toAppendStream(provinceStateTable, ProvinceStats.class);

        //TODO 5、打印数据并写入clickhouse
        provinceStatsDataStream.print("result>>>>>>>>>>>");
        provinceStatsDataStream.addSink(ClickHouseUtil.<ProvinceStats>getSink("insert into province_stats_2021 values(?,?,?,?,?,?,?,?,?,?)"));

        //TODO 6、启动任务
        env.execute("ProvinceStatsSqlApp");

    }
}
