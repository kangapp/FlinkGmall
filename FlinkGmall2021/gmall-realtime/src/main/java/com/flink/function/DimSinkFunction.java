package com.flink.function;

import com.alibaba.fastjson.JSONObject;
import com.flink.common.GmallConfig;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Set;

public class DimSinkFunction extends RichSinkFunction<JSONObject> {

    private Connection connection;

    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
        //phoenix默认不自动提交
        connection.setAutoCommit(true);
    }

    @Override
    public void invoke(JSONObject value, Context context) throws Exception {

        PreparedStatement preparedStatement = null;
        try {
            //获取SQL语句
            String upsertSql = getUpsertSql(value.getString("sinkTable"),
                    value.getJSONObject("after"));
            System.out.println(upsertSql);

            //预编译SQL
            preparedStatement = connection.prepareStatement(upsertSql);

            //执行插入操作
            preparedStatement.executeUpdate();

//            connection.commit();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if(preparedStatement != null) {
                preparedStatement.close();
            }
        }

    }

    //SQL: upsert into tn(id,tm_name) values(..)
    private String getUpsertSql(String sinkTable, JSONObject after) {
        StringBuffer sql = new StringBuffer();
        Set<String> keySet = after.keySet();
        Collection<Object> values = after.values();
        sql.append("upsert into ").append(sinkTable).append("(")
                .append(StringUtils.join(keySet,",")).append(")values('")
                .append(StringUtils.join(values,"','")).append("')");

        return sql.toString();
    }
}
