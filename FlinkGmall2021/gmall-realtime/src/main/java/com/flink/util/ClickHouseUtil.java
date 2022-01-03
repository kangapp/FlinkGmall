package com.flink.util;

import com.flink.bean.TransientSink;
import com.flink.common.GmallConfig;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.lang.reflect.Field;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class ClickHouseUtil {

    public static <T> SinkFunction<T> getSink(String sql) {
        return JdbcSink.sink(sql,
                new JdbcStatementBuilder<T>() {
                    @Override
                    public void accept(PreparedStatement preparedStatement, T t) throws SQLException {
                        try{
                            //获取所有的属性信息
                            Field[] declaredFields = t.getClass().getDeclaredFields();

                            int offset = 0;
                            for (int i = 0; i< declaredFields.length; i++) {
                                Field field = declaredFields[i];
                                //设置私有属性可访问
                                field.setAccessible(true);

                                //获取字段上的注解
                                TransientSink annotation = field.getAnnotation(TransientSink.class);
                                if (annotation != null) {
                                    offset++;
                                    continue;
                                }
                                Object value = field.get(t);
                                preparedStatement.setObject(i+1-offset, value);
                            }
                        } catch (IllegalAccessException e) {
                            e.printStackTrace();
                        }

                    }
                },new JdbcExecutionOptions.Builder().withBatchSize(5).build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withDriverName(GmallConfig.CLICKHOUSE_DRIVER)
                        .withUrl(GmallConfig.CLICKHOUSE_URL).build());
    }
}
