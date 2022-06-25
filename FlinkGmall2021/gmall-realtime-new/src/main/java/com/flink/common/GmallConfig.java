package com.flink.common;

public class GmallConfig {

    //Phoenix 库名
    public static final String HBASE_SCHEMA = "GMALL";
    //Phoenix 驱动
    public static final String PHOENIX_DRIVER = "org.apache.phoenix.jdbc.PhoenixDriver";
    //Phoenix 连接参数
    public static final String PHOENIX_SERVER = "jdbc:phoenix:hadoop100,hadoop101,hadoop102:2181";

    public static final String CLICKHOUSE_URL="jdbc:clickhouse://localhost:8123/default";
    public static final String CLICKHOUSE_DRIVER = "ru.yandex.clickhouse.ClickHouseDriver";
}
