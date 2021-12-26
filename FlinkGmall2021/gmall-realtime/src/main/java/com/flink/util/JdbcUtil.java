package com.flink.util;

import com.google.common.base.CaseFormat;
import org.apache.commons.beanutils.BeanUtils;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class JdbcUtil {

    public static <T> List<T> queryList(Connection connection, String querySql, Class<T> clz, boolean underScoreToCamel) throws Exception {

        //创建集合用于存放查询结果数据
        ArrayList<T> resultList = new ArrayList<>();

        PreparedStatement preparedStatement = connection.prepareStatement(querySql);

        ResultSet resultSet = preparedStatement.executeQuery();

        //解析ResultSet
        ResultSetMetaData metaData = resultSet.getMetaData();
        int columnCount = metaData.getColumnCount();
        while (resultSet.next()) {
            //创建泛型对象
            T t = clz.newInstance();

            //给泛型对象赋值
            for (int i=1; i<= columnCount; i++) {
                //获取列名
                String columnName = metaData.getColumnName(i);

                //判断是否需要转换为驼峰命名
                if (underScoreToCamel) {
                    columnName = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, columnName.toLowerCase());
                }

                //获取列值
                Object value = resultSet.getObject(i);

                //给泛型对象赋值
                BeanUtils.setProperty(t, columnName, value);

            }
            //将对象添加到集合
            resultList.add(t);
        }
        preparedStatement.close();
        resultSet.close();

        return resultList;
    }
}
