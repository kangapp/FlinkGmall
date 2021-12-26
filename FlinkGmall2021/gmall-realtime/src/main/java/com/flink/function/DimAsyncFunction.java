package com.flink.function;

import com.alibaba.fastjson.JSONObject;
import com.flink.common.GmallConfig;
import com.flink.util.DimUtil;
import com.flink.util.ThreadPoolUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Collections;
import java.util.concurrent.ThreadPoolExecutor;

@Slf4j
public abstract class DimAsyncFunction<T> extends RichAsyncFunction<T, T> implements DimAsyncJoinFunction<T> {

    private Connection connection;
    private ThreadPoolExecutor threadPoolExecutor;
    private String tableName;

    public DimAsyncFunction(String tableName) {
        this.tableName = tableName;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
        threadPoolExecutor = ThreadPoolUtil.getThreadPool();
    }

    @Override
    public void asyncInvoke(T input, ResultFuture<T> resultFuture) {
        threadPoolExecutor.submit(() -> {
            try {
                String id = getKey(input);
                // 查询维度信息
                JSONObject dimInfo = DimUtil.getDimInfo(connection, tableName, id);

                //补充维度信息
                if (dimInfo != null) {
                    join(input, dimInfo);
                }

                //将数据输出
                resultFuture.complete(Collections.singletonList(input));
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
    }

    @Override
    public void timeout(T input, ResultFuture<T> resultFuture) {
        System.out.println("数据超时：" + input);
    }
}
