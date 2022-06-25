package com.flink.utils;

import com.alibaba.fastjson.JSONObject;
import com.flink.common.GmallConfig;
import org.apache.commons.lang3.StringUtils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Set;

public class PhoenixUtil {

    // 定义连接对象
    private static Connection conn;

    /**
     * 初始化SQL执行环境
     */
    public static void initializeConnection() {
        try {
            Class.forName(GmallConfig.PHOENIX_DRIVER);
            conn = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
            conn.setSchema(GmallConfig.HBASE_SCHEMA);
        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        }
    }

    public static void insertValues(String sinkTable, JSONObject data) {
        if(conn == null) {
            synchronized (PhoenixUtil.class) {
                if(conn == null) {
                    initializeConnection();
                }

                //获取字段名
                Set<String> columns = data.keySet();

                //获取字段对应的值
                Collection<Object> values = data.values();

                //拼接字段名
                String columnStr = StringUtils.join(columns, ",");

                // 拼接字段值
                String valueStr = StringUtils.join(values, "','");

                //拼接插入语句
                String sql = "upsert into " + GmallConfig.HBASE_SCHEMA + "." + sinkTable
                        + "(" +columnStr + ") values ('" + valueStr + "')";

                PreparedStatement preparedStatement = null;

                try {
                    System.out.println("插入语句：" + sql);
                    preparedStatement = conn.prepareStatement(sql);
                    preparedStatement.execute();
                    conn.commit();
                } catch (SQLException throwables) {
                    throwables.printStackTrace();
                    throw new RuntimeException("数据库操作对象获取或执行异常");
                } finally {
                    if(preparedStatement != null) {
                        try {
                            preparedStatement.close();
                        } catch (SQLException throwables) {
                            throwables.printStackTrace();
                            throw new RuntimeException("数据库操作对象释放异常");
                        }
                    }
                }
            }
        }
    }
}
